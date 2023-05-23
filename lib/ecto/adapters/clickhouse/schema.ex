defmodule Ecto.Adapters.ClickHouse.Schema do
  @moduledoc false
  @conn Ecto.Adapters.ClickHouse.Connection
  @dialyzer :no_improper_lists

  # ignores passing stream into Ecto.Adapters.SQL.query!
  @dialyzer {:no_fail_call, do_insert_stream: 7}
  @dialyzer {:no_return, insert_stream: 4, do_insert_stream: 7}

  def insert_all(
        adapter_meta,
        schema_meta,
        header,
        rows,
        on_conflict,
        returning,
        placeholders,
        opts
      ) do
    %{source: source, prefix: prefix, schema: schema} = schema_meta
    opts = [{:command, :insert} | opts]

    %{num_rows: num_rows} =
      case rows do
        {%Ecto.Query{} = _query, params} ->
          sql = @conn.insert(prefix, source, header, rows, on_conflict, returning, placeholders)
          Ecto.Adapters.SQL.query!(adapter_meta, sql, params, opts)

        rows when is_list(rows) ->
          names = prepare_names(header)
          types = prepare_types(schema, header, opts)
          opts = [{:names, names}, {:types, types} | opts]
          sql = [@conn.insert(prefix, source, [], []) | " FORMAT RowBinaryWithNamesAndTypes"]
          rows = unzip_rows(rows, header)
          Ecto.Adapters.SQL.query!(adapter_meta, sql, rows, opts)
      end

    {num_rows, nil}
  end

  def insert(adapter_meta, schema_meta, params, opts) do
    %{source: source, prefix: prefix, schema: schema} = schema_meta
    {header, row} = :lists.unzip(params)

    names = prepare_names(header)
    types = prepare_types(schema, header, opts)
    sql = [@conn.insert(prefix, source, [], []) | " FORMAT RowBinaryWithNamesAndTypes"]
    opts = [{:command, :insert}, {:names, names}, {:types, types} | opts]

    Ecto.Adapters.SQL.query!(adapter_meta, sql, [row], opts)
    {:ok, []}
  end

  def insert_stream(repo, schema, rows, opts) when is_atom(schema) do
    header = schema.__schema__(:fields)

    do_insert_stream(
      repo,
      schema.__schema__(:prefix),
      schema.__schema__(:source),
      header,
      extract_types(schema, header),
      rows,
      opts
    )
  end

  def insert_stream(repo, table, rows, opts) when is_binary(table) do
    types = Keyword.fetch!(opts, :types)
    {header, types} = Enum.unzip(types)
    do_insert_stream(repo, nil, table, header, types, rows, opts)
  end

  def insert_stream(repo, {source, schema}, rows, opts) when is_atom(schema) do
    header = schema.__schema__(:fields)

    do_insert_stream(
      repo,
      schema.__schema__(:prefix),
      source,
      header,
      extract_types(schema, header),
      rows,
      opts
    )
  end

  defp do_insert_stream(repo, prefix, source, header, types, rows, opts) do
    chunk_every = opts[:chunk_every] || 1000

    names = prepare_names(header)
    opts = [{:command, :insert}, {:encode, false} | opts]
    sql = [@conn.insert(prefix, source, [], []) | " FORMAT RowBinaryWithNamesAndTypes"]

    row_binary =
      rows
      |> Stream.chunk_every(chunk_every)
      |> Stream.map(fn chunk ->
        chunk
        |> unzip_rows(header)
        |> Ch.RowBinary.encode_rows(types)
      end)

    stream = Stream.concat([Ch.RowBinary.encode_names_and_types(names, types)], row_binary)
    %{num_rows: num_rows} = Ecto.Adapters.SQL.query!(repo, sql, stream, opts)

    {num_rows, nil}
  end

  def delete(adapter_meta, schema_meta, params, opts) do
    %{source: source, prefix: prefix} = schema_meta
    filter_values = Keyword.values(params)
    sql = @conn.delete(prefix, source, params, [])
    Ecto.Adapters.SQL.query!(adapter_meta, sql, filter_values, opts)
    {:ok, []}
  end

  defp prepare_names(header) do
    Enum.map(header, &String.Chars.Atom.to_string/1)
  end

  defp extract_types(schema, fields) do
    Enum.map(fields, fn field ->
      type = schema.__schema__(:type, field) || raise "missing type for field " <> inspect(field)
      type |> Ecto.Type.type() |> remap_type(type, schema, field)
    end)
  end

  defp prepare_types(schema, header, opts) do
    cond do
      schema ->
        extract_types(schema, header)

      types = opts[:types] ->
        Enum.map(header, fn field -> Access.fetch!(types, field) end)

      true ->
        raise ArgumentError, "missing :types"
    end
  end

  defp remap_type({:parameterized, Ch, t}, _original, _schema, _field), do: t

  defp remap_type(t, _original, _schema, _field)
       when t in [:string, :date, :uuid, :boolean],
       do: t

  defp remap_type(dt, _original, _schema, _field)
       when dt in [:naive_datetime, :utc_datetime],
       do: :datetime

  defp remap_type(usec, _original, _schema, _field)
       when usec in [:naive_datetime_usec, :utc_datetime_usec],
       do: {:datetime64, _precision = 6}

  # TODO remove
  defp remap_type(t, _original, _schema, _field)
       when t in [:binary, :binary_id],
       do: :string

  # TODO remove
  for size <- [8, 16, 32, 64, 128, 256] do
    defp remap_type(unquote(:"u#{size}") = u, _original, _schema, _field), do: u
    defp remap_type(unquote(:"i#{size}") = i, _original, _schema, _field), do: i
  end

  defp remap_type({:array = a, t}, original, schema, field),
    do: {a, remap_type(t, original, schema, field)}

  defp remap_type(:integer, _original, Ecto.Migration.SchemaMigration, :version), do: :i64

  defp remap_type(time, _original, _schema, _field) when time in [:time, :time_usec] do
    raise ArgumentError,
          "`#{inspect(time)}` type is not supported as there is no `Time` type in ClickHouse."
  end

  defp remap_type(other, original, schema, field) do
    ch_type = ch_type_hint(original)

    raise ArgumentError, """
    #{inspect(other)} type is ambiguous, please use `Ch` Ecto type instead.

    Example:

        schema "#{schema.__schema__(:source)}" do
          field :#{field}, Ch, type: "#{ch_type}"
        end

    You can also try using `ecto.ch.schema` to generate a schema:

        mix ecto.ch.schema <database>.#{schema.__schema__(:source)}

    """
  end

  # https://hexdocs.pm/ecto/Ecto.Schema.html#module-primitive-types
  defp ch_type_hint(:id), do: "Int64"
  defp ch_type_hint(:integer), do: "Int64"
  defp ch_type_hint(:float), do: "Float32"
  defp ch_type_hint({:array, type}), do: "{:array, #{ch_type_hint(type)}}"
  defp ch_type_hint(:map), do: "Map(String, Int64)"
  defp ch_type_hint({:map, type}), do: "Map(String, #{ch_type_hint(type)})"
  defp ch_type_hint(:decimal), do: "Decimal32(2)"

  defp ch_type_hint(type) do
    case Ecto.Type.type(type) do
      ^type -> raise ArgumentError, "unknown type: `#{inspect(type)}`"
      type -> ch_type_hint(type)
    end
  end

  defp unzip_rows([row | rows], header) when is_list(row) do
    [unzip_row_list(header, row) | unzip_rows(rows, header)]
  end

  defp unzip_rows([row | rows], header) when is_map(row) do
    [unzip_row_map(header, row) | unzip_rows(rows, header)]
  end

  defp unzip_rows([], _header), do: []

  defp unzip_row_list([field | fields], row) do
    case List.keyfind(row, field, 0) do
      {_, value} -> [value | unzip_row_list(fields, row)]
      nil = not_found -> [not_found | unzip_row_list(fields, row)]
    end
  end

  defp unzip_row_list([], _row), do: []

  defp unzip_row_map([field | fields], row) do
    case Map.get(row, field) do
      nil = not_found -> [not_found | unzip_row_map(fields, row)]
      value -> [value | unzip_row_map(fields, row)]
    end
  end

  defp unzip_row_map([], _row), do: []
end
