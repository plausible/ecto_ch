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
    %{source: source, prefix: prefix} = schema_meta
    opts = [{:command, :insert} | opts]

    %{num_rows: num_rows} =
      case rows do
        {%Ecto.Query{} = _query, params} ->
          sql =
            @conn.insert(prefix, source, header, rows, on_conflict, returning, placeholders, opts)

          Ecto.Adapters.SQL.query!(adapter_meta, sql, params, opts)

        rows when is_list(rows) ->
          if input = opts[:input] do
            insert_input(adapter_meta, schema_meta, input, rows, opts)
          else
            insert_rows(adapter_meta, schema_meta, header, rows, opts)
          end
      end

    {num_rows, nil}
  end

  defp insert_rows(adapter_meta, schema_meta, header, rows, opts) do
    %{source: source, prefix: prefix, schema: schema} = schema_meta

    names = prepare_names(header)
    types = prepare_types(schema, header, opts)
    opts = [{:names, names}, {:types, types} | opts]
    rows = unzip_rows(rows, header)
    sql = [@conn.insert(prefix, source, header, []) | " FORMAT RowBinaryWithNamesAndTypes"]

    Ecto.Adapters.SQL.query!(adapter_meta, sql, rows, opts)
  end

  defp insert_input(adapter_meta, schema_meta, query, rows, opts) do
    %{repo: repo, adapter: adapter} = adapter_meta
    %{source: source, prefix: prefix} = schema_meta

    types =
      case query.from.source do
        {:fragment, [{:input, types}], _fragment} ->
          types

        _other ->
          raise ArgumentError, """
          unexpected source found in input query:

            #{inspect(query)}

          Please use Ecto.Adapters.ClickHouse.API.input/1 helper in :input queries:

            import Ecto.Adapters.ClickHouse.API, only: [input: 1]
            input = from i in input(schema_or_types), select: %{a: i.a}
            insert_all("table", rows, input: input)

          """
      end

    {query, opts} = repo.prepare_query(:insert_all, query, opts)

    {query, _cast_params, dump_params} =
      Ecto.Adapter.Queryable.plan_query(:insert_all, adapter, query)

    unless dump_params == [] do
      raise ArgumentError, """
      cannot insert from parameterized input query:

        #{inspect(query)}

      Please remove all parameters from the query.
      """
    end

    ix =
      case query.select do
        %Ecto.Query.SelectExpr{expr: {:&, _, [ix]}} -> ix
        _ -> nil
      end

    header =
      case query.select do
        %Ecto.Query.SelectExpr{expr: {:%{}, _ctx, args}} ->
          Enum.map(args, &elem(&1, 0))

        %Ecto.Query.SelectExpr{take: %{^ix => {_fun, fields}}} ->
          fields

        # TODO might need more work
        %Ecto.Query.SelectExpr{expr: {:merge, _, [{:&, _, [_]}, {:%{}, _, args}]}} ->
          fields = Keyword.keys(types)
          merged = Enum.map(args, &elem(&1, 0))
          Enum.uniq(fields ++ merged)

        _ ->
          raise ArgumentError, """
          cannot generate a fields list for insert_all from the given input query
          because it does not have a select clause that uses a map:

            #{inspect(query)}

          Please add a select clause that selects into a map, like this:

            import Ecto.Adapters.ClickHouse.API, only: [input: 1]

            from i in input(Source),
              select: %{
                field_a: i.bar,
                field_b: i.foo
              }

          All keys must exist in the schema that is being inserted into
          """
      end

    sql = [
      @conn.insert(prefix, source, header, {query, dump_params})
      | " FORMAT RowBinaryWithNamesAndTypes"
    ]

    names = prepare_names(header)
    types = prepare_types(_schema = nil, header, types: types)
    opts = [{:names, names}, {:types, types} | opts]
    rows = unzip_rows(rows, header)

    Ecto.Adapters.SQL.query!(adapter_meta, sql, rows, opts)
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

    types = Ch.RowBinary.encoding_types(types)
    names = prepare_names(header)
    opts = [{:command, :insert}, {:encode, false} | opts]
    sql = [@conn.insert(prefix, source, [], []) | " FORMAT RowBinaryWithNamesAndTypes"]

    row_binary =
      rows
      |> Stream.chunk_every(chunk_every)
      |> Stream.map(fn chunk ->
        chunk
        |> unzip_rows(header)
        |> Ch.RowBinary._encode_rows(types)
      end)

    stream = Stream.concat([Ch.RowBinary.encode_names_and_types(names, types)], row_binary)
    %{num_rows: num_rows} = Ecto.Adapters.SQL.query!(repo, sql, stream, opts)

    {num_rows, nil}
  end

  def update(adapter_meta, schema_meta, fields, params, returning, opts) do
    %{source: source, prefix: prefix, schema: schema} = schema_meta
    all_fields = fields ++ params
    values = Keyword.values(all_fields)
    types = schema |> extract_types(Keyword.keys(all_fields)) |> Enum.map(&update_param_type/1)

    {expressions, {_count, all_params}} =
      Enum.map_reduce(Enum.zip(types, values), {0, []}, fn {type, value}, acc ->
        {expression, acc} = update_param(type, value, acc)
        {{:expression, expression}, acc}
      end)

    sql = @conn.update(prefix, source, fields, params, returning, expressions)
    all_params = Enum.reverse(all_params)
    Ecto.Adapters.SQL.query!(adapter_meta, sql, all_params, opts)
    {:ok, []}
  end

  defp update_param_type({:low_cardinality, type}), do: update_param_type(type)
  defp update_param_type({:simple_aggregate_function, _name, type}), do: update_param_type(type)
  # Assignment casts need clock text; Time64 constants can render as fractional seconds.
  defp update_param_type(:time), do: :string
  defp update_param_type({:time64, _precision}), do: :string
  defp update_param_type({:nullable, type}), do: {:nullable, update_param_type(type)}
  defp update_param_type({:array, type}), do: {:array, update_param_type(type)}
  defp update_param_type({:tuple, types}), do: {:tuple, Enum.map(types, &update_param_type/1)}

  defp update_param_type({:map, key_type, value_type}),
    do: {:map, update_param_type(key_type), update_param_type(value_type)}

  defp update_param_type(type), do: type

  defp update_param(type, nil, acc)
       when type == :dynamic or (is_tuple(type) and elem(type, 0) == :variant) do
    {expression, acc} = bind_update_param({:nullable, :nothing}, nil, acc)
    {cast_update_param(expression, type), acc}
  end

  defp update_param(:dynamic, [], acc), do: {cast_update_param("[]", :dynamic), acc}

  defp update_param(:dynamic, value, acc) do
    type = @conn.infer_param_type(value)
    {expression, acc} = bind_update_param({:variant, [type]}, value, acc)
    {cast_update_param(expression, :dynamic), acc}
  end

  defp update_param({:variant, types} = type, value, acc) do
    candidate =
      Enum.find(types, fn candidate -> match?({:ok, ^value}, Ch.cast(value, candidate)) end) ||
        raise ArgumentError, "value #{inspect(value)} does not match #{inspect(type)}"

    {expression, acc} =
      if nested_dynamic_type?(candidate) do
        update_param(candidate, value, acc)
      else
        # A single-member Variant preserves its member across assignment casts.
        bind_update_param({:variant, [candidate]}, value, acc)
      end

    {cast_update_param(expression, type), acc}
  end

  defp update_param({:array, type} = array, values, acc) when is_list(values) do
    if nested_dynamic_type?(type) do
      {expressions, acc} = Enum.map_reduce(values, acc, &update_param(type, &1, &2))
      expression = [?[, Enum.intersperse(expressions, ?,), ?]]

      expression =
        if values == [],
          do: cast_update_param(expression, empty_update_param_type(array)),
          else: expression

      {cast_update_param(expression, array), acc}
    else
      bind_update_param(array, values, acc)
    end
  end

  defp update_param({:tuple, types} = tuple, values, acc) when is_tuple(values) do
    if nested_dynamic_type?(tuple) do
      {expressions, acc} =
        Enum.map_reduce(Enum.zip(types, Tuple.to_list(values)), acc, fn {type, value}, acc ->
          update_param(type, value, acc)
        end)

      expression = ["tuple(", Enum.intersperse(expressions, ?,), ?)]
      {cast_update_param(expression, tuple), acc}
    else
      bind_update_param(tuple, values, acc)
    end
  end

  defp update_param({:map, key_type, value_type} = map, values, acc) when is_map(values) do
    if nested_dynamic_type?(map) do
      {expressions, acc} =
        Enum.map_reduce(values, acc, fn {key, value}, acc ->
          {key_expression, acc} = update_param(key_type, key, acc)
          {value_expression, acc} = update_param(value_type, value, acc)
          {[key_expression, ?,, value_expression], acc}
        end)

      expression = ["map(", Enum.intersperse(expressions, ?,), ?)]

      expression =
        if map_size(values) == 0,
          do: cast_update_param(expression, empty_update_param_type(map)),
          else: expression

      {cast_update_param(expression, map), acc}
    else
      bind_update_param(map, values, acc)
    end
  end

  defp update_param(type, value, acc), do: bind_update_param(type, value, acc)

  defp bind_update_param(type, value, {idx, params}) do
    expression = ["{$", Integer.to_string(idx), ?:, Ch.Types.encode(type), ?}]
    {expression, {idx + 1, [update_param_value(type, value) | params]}}
  end

  defp cast_update_param(expression, type) do
    type = type |> Ch.Types.encode() |> IO.iodata_to_binary()
    ["CAST(", expression, " AS ", type, ?)]
  end

  defp nested_dynamic_type?(:dynamic), do: true
  defp nested_dynamic_type?({:variant, _types}), do: true
  defp nested_dynamic_type?({:array, type}), do: nested_dynamic_type?(type)
  defp nested_dynamic_type?({:nullable, type}), do: nested_dynamic_type?(type)
  defp nested_dynamic_type?({:tuple, types}), do: Enum.any?(types, &nested_dynamic_type?/1)

  defp nested_dynamic_type?({:map, key_type, value_type}),
    do: nested_dynamic_type?(key_type) or nested_dynamic_type?(value_type)

  defp nested_dynamic_type?(_type), do: false

  # Empty Dynamic containers need a concrete source type for ClickHouse's assignment cast.
  defp empty_update_param_type(:dynamic), do: :string

  defp empty_update_param_type({kind, type}) when kind in [:array, :nullable],
    do: {kind, empty_update_param_type(type)}

  defp empty_update_param_type({:tuple, types}),
    do: {:tuple, Enum.map(types, &empty_update_param_type/1)}

  defp empty_update_param_type({:map, key, value}),
    do: {:map, empty_update_param_type(key), empty_update_param_type(value)}

  defp empty_update_param_type(type), do: type

  defp update_param_value(_type, nil), do: nil
  defp update_param_value({:nullable, type}, value), do: update_param_value(type, value)
  defp update_param_value({:variant, [type]}, value), do: update_param_value(type, value)
  defp update_param_value(:json, value), do: Jason.encode!(value)
  defp update_param_value(_type, %Time{} = value), do: Time.to_iso8601(value)

  defp update_param_value({enum, mappings}, value)
       when enum in [:enum8, :enum16] and is_integer(value) do
    case List.keyfind(mappings, value, 1) do
      {label, ^value} ->
        label

      nil ->
        raise ArgumentError,
              "enum value #{inspect(value)} not found in mapping: #{inspect(mappings)}"
    end
  end

  defp update_param_value(type, value) when type in [:ipv4, :ipv6] and is_tuple(value),
    do: value |> :inet.ntoa() |> to_string()

  defp update_param_value({:array, type}, values),
    do: Enum.map(values, &update_param_value(type, &1))

  defp update_param_value({:tuple, types}, values) when is_tuple(values),
    do: update_param_value({:tuple, types}, Tuple.to_list(values))

  defp update_param_value({:tuple, types}, values),
    do: types |> Enum.zip_with(values, &update_param_value/2) |> List.to_tuple()

  defp update_param_value({:map, key_type, value_type}, values) do
    Map.new(values, fn {key, value} ->
      {update_param_value(key_type, key), update_param_value(value_type, value)}
    end)
  end

  defp update_param_value(_type, value), do: value

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
      type =
        schema.__schema__(:type, field) || find_field_source_type(schema, field) ||
          raise "missing type for field " <> inspect(field)

      remap_type(type, schema, field)
    end)
  end

  defp find_field_source_type(schema, field) do
    reverse_field_source =
      Enum.find(schema.__schema__(:fields), &(schema.__schema__(:field_source, &1) == field))

    if reverse_field_source do
      schema.__schema__(:type, reverse_field_source)
    end
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

  @doc false
  def remap_type(type, schema, field) do
    remap_type(type, type, schema, field)
  end

  defp remap_type({:parameterized, {Ch, t}}, _original, _schema, _field), do: t

  defp remap_type({:parameterized, {module, params}}, original, schema, field) do
    type = module.type(params)
    remap_type(type, original, schema, field)
  end

  defp remap_type(t, _original, _schema, _field)
       when t in [:string, :date, :time, :uuid, :boolean],
       do: t

  defp remap_type(Ecto.UUID, _original, _schema, _field), do: :uuid

  defp remap_type(dt, _original, _schema, _field)
       when dt in [:naive_datetime, :utc_datetime],
       do: :datetime

  defp remap_type(usec, _original, _schema, _field)
       when usec in [:naive_datetime_usec, :utc_datetime_usec],
       do: {:datetime64, _precision = 6}

  defp remap_type(:time_usec, _original, _schema, _field), do: {:time64, _precision = 6}

  defp remap_type(:bitstring, _original, _schema, _field) do
    raise ArgumentError, "type :bitstring is not supported"
  end

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

  defp remap_type(type, original, schema, field) do
    case Ecto.Type.type(type) do
      ^type ->
        raise ArgumentError, """
        #{inspect(type)} type is ambiguous, please use `Ch` Ecto type instead.

        Example:

            schema "#{schema.__schema__(:source)}" do
              field :#{field}, Ch, type: "#{ch_type_hint(original)}"
            end

        You can also try using `ecto.ch.schema` to generate a schema:

            mix ecto.ch.schema <database>.#{schema.__schema__(:source)}

        """

      type ->
        remap_type(type, original, schema, field)
    end
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
