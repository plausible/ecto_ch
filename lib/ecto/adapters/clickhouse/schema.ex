defmodule Ecto.Adapters.ClickHouse.Schema do
  @moduledoc false
  @conn Ecto.Adapters.ClickHouse.Connection
  @dialyzer :no_improper_lists

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

  defp unzip_rows([row | rows], header) do
    [unzip_row(header, row) | unzip_rows(rows, header)]
  end

  defp unzip_rows([], _header), do: []

  defp unzip_row([field | fields], row) do
    case List.keyfind(row, field, 0) do
      {_, value} -> [value | unzip_row(fields, row)]
      nil = not_found -> [not_found | unzip_row(fields, row)]
    end
  end

  defp unzip_row([], _row), do: []
end
