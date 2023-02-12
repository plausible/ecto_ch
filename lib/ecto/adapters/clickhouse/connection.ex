defmodule Ecto.Adapters.ClickHouse.Connection do
  @moduledoc false

  def child_spec(opts) do
    Ch.child_spec(opts)
  end

  def prepare_execute(conn, _name, statement, params, opts) do
    query = Ch.Query.build(statement, opts)
    DBConnection.prepare_execute(conn, query, params, opts)
  end

  def execute(conn, query, params, opts) do
    DBConnection.execute(conn, query, params, opts)
  end

  def query(conn, statement, params, opts) do
    Ch.query(conn, statement, params, opts)
  end

  alias Ecto.{Query, SubQuery}
  alias Ecto.Query.{QueryExpr, JoinExpr, BooleanExpr, Tagged}

  def all(query, params) do
    sources = create_names(query)
    from = from(query, sources, params)
    select = select(query, sources, params)
    join = join(query, sources, params)
    where = where(query, sources, params)
    group_by = group_by(query, sources, params)
    having = having(query, sources, params)
    order_by = order_by(query, sources, params)
    limit = limit(query, sources, params)
    offset = offset(query, sources, params)
    [select, from, join, where, group_by, having, order_by, limit, offset]
  end

  alias Ecto.Migration.{Table, Reference}

  def ddl_logs(_), do: []

  @spec execute_ddl(Ecto.Adapter.Migration.command()) :: iodata
  def execute_ddl({command, %Table{} = table, columns})
      when command in [:create, :create_if_not_exists] do
    columns =
      case columns do
        [{_, :id, _, _} | columns] -> columns
        _ -> columns
      end

    [
      [
        if command == :create_if_not_exists do
          "CREATE TABLE IF NOT EXISTS "
        else
          "CREATE TABLE "
        end,
        quote_table(table.prefix, table.name),
        ?(,
        intersperse_map(columns, ", ", &ddl_column_definition/1),
        ?),
        ddl_options_expr(table.options),
        " ENGINE=",
        table.engine || "TinyLog"
      ]
    ]
  end

  def execute_ddl({command, %Table{} = table}) when command in [:drop, :drop_if_exists] do
    [
      [
        if command == :drop_if_exists do
          "DROP TABLE IF EXISTS "
        else
          "DROP TABLE "
        end,
        quote_table(table.prefix, table.name)
      ]
    ]
  end

  def execute_ddl({:alter, %Table{} = table, changes}) do
    Enum.map(changes, fn change ->
      ["ALTER TABLE ", quote_table(table.prefix, table.name), ?\s | ddl_column_change(change)]
    end)
  end

  # TODO from clickhouse_ecto: Add 'ON CLUSTER' option.
  # https://github.com/clickhouse-elixir/clickhouse_ecto/blob/bc963d3f53bf4264e6fa2b7345c7e3c3dc49d6ee/lib/clickhouse_ecto/migration.ex#L56
  def execute_ddl({:rename, %Table{} = current_table, %Table{} = new_table}) do
    [
      [
        "RENAME TABLE ",
        quote_name([current_table.prefix, current_table.name]),
        " TO ",
        quote_name(new_table.name)
      ]
    ]
  end

  def execute_ddl({:rename, %Table{} = table, current_column, new_column}) do
    [
      [
        "ALTER TABLE ",
        quote_table(table.prefix, table.name),
        " RENAME COLUMN ",
        current_column,
        " TO ",
        new_column
      ]
    ]
  end

  def execute_ddl(string) when is_binary(string) do
    [string]
  end

  def execute_ddl(keyword) when is_list(keyword) do
    error!(nil, "Ecto.Adapters.ClickHouse does not support keyword lists in execute_ddl")
  end

  defp ddl_column_definition({:add, name, %Reference{} = ref, opts}) do
    [quote_name(name), ?\s | ddl_column_options(ref.type, opts)]
  end

  defp ddl_column_definition({:add, name, type, opts}) do
    [
      quote_name(name),
      ?\s,
      ddl_column_type(type, opts),
      ?\s
      | ddl_column_options(type, opts)
    ]
  end

  defp ddl_options_expr(nil), do: []
  defp ddl_options_expr(options) when is_binary(options), do: options

  defp ddl_options_expr([{_k, _v} | _]) do
    error!(nil, "Ecto.Adapter.ClickHouse does not support keyword lists in :options")
  end

  defp ddl_column_options(type, opts) do
    default = Keyword.fetch(opts, :default)
    null = Keyword.get(opts, :null)
    [ddl_default_expr(default, type), ?\s | ddl_null_expr(null)]
  end

  defp ddl_null_expr(true), do: "NULL"
  defp ddl_null_expr(_), do: []

  @dialyzer {:no_improper_lists, ddl_default_expr: 2}
  defp ddl_default_expr(:error, _), do: []

  defp ddl_default_expr({:ok, nil}, _type), do: error!(nil, "NULL is not supported")
  defp ddl_default_expr({:ok, []}, _type), do: error!(nil, "arrays are not supported")

  defp ddl_default_expr({:ok, literal}, _type) when is_binary(literal) do
    ["DEFAULT '", escape_string(literal), ?']
  end

  defp ddl_default_expr({:ok, literal}, _type) when is_number(literal) do
    ["DEFAULT " | to_string(literal)]
  end

  defp ddl_default_expr({:ok, literal}, _type) when is_boolean(literal) do
    ["DEFAULT " | to_string(if literal, do: 1, else: 0)]
  end

  defp ddl_default_expr({:ok, {:fragment, expr}}, _type) do
    ["DEFAULT ", expr]
  end

  defp ddl_default_expr({:ok, expr}, type) do
    raise ArgumentError,
          "unknown default `#{inspect(expr)}` for type `#{inspect(type)}`. " <>
            ":default may be a string, number, boolean, empty list or a fragment(...)"
  end

  @dialyzer {:no_improper_lists, ddl_column_type: 2}
  defp ddl_column_type({:array, type}, opts) do
    [ddl_column_type(type, opts) | "[]"]
  end

  defp ddl_column_type(type, opts) do
    size = Keyword.get(opts, :size)
    precision = Keyword.get(opts, :precision)
    scale = Keyword.get(opts, :scale)
    type_name = ecto_to_db(type)

    cond do
      size -> [type_name, ?(, to_string(size), ?)]
      precision -> [type_name, ?(, to_string(precision), ?,, to_string(scale || 0), ?)]
      true -> type_name
    end
  end

  defp ddl_column_change({:add, name, %Reference{} = ref, opts}) do
    ["ADD COLUMN ", quote_name(name), ?\s | ddl_column_options(ref.type, opts)]
  end

  defp ddl_column_change({:add, name, type, opts}) do
    [
      "ADD COLUMN ",
      quote_name(name),
      ?\s,
      ddl_column_type(type, opts),
      ?\s
      | ddl_column_options(type, opts)
    ]
  end

  defp ddl_column_change({:modify, name, %Reference{} = ref, opts}) do
    [
      "MODIFY COLUMN ",
      quote_name(name),
      ?\s,
      ddl_modify_null(name, opts),
      ?\s
      | ddl_modify_default(name, ref.type, opts)
    ]
  end

  defp ddl_column_change({:modify, name, type, opts}) do
    [
      "MODIFY COLUMN ",
      quote_name(name),
      ?\s,
      ddl_column_type(type, opts),
      ?\s,
      ddl_modify_null(name, opts),
      ?\s
      | ddl_modify_default(name, type, opts)
    ]
  end

  defp ddl_column_change({:remove, name}) do
    ["DROP COLUMN ", quote_name(name)]
  end

  defp ddl_modify_null(_name, opts) do
    case Keyword.get(opts, :null) do
      nil -> []
      val -> ddl_null_expr(val)
    end
  end

  defp ddl_modify_default(name, type, opts) do
    case Keyword.fetch(opts, :default) do
      {:ok, val} ->
        [" ADD ", ddl_default_expr({:ok, val}, type), " FOR ", quote_name(name)]

      :error ->
        []
    end
  end

  # TODO support insert into ... select ... from
  def insert(prefix, table, header) do
    fields = [?(, intersperse_map(header, ?,, &quote_name/1), ?)]
    ["INSERT INTO ", quote_table(prefix, table) | fields]
  end

  binary_ops = [
    ==: " = ",
    !=: " != ",
    <=: " <= ",
    >=: " >= ",
    <: " < ",
    >: " > ",
    and: " AND ",
    or: " OR ",
    ilike: " ILIKE ",
    like: " LIKE ",
    in: " IN ",
    is_nil: " WHERE "
  ]

  @binary_ops Keyword.keys(binary_ops)

  for {op, str} <- binary_ops do
    defp handle_call(unquote(op), 2), do: {:binary_op, unquote(str)}
  end

  defp handle_call(fun, _arity), do: {:fun, Atom.to_string(fun)}

  defp select(%Query{select: %{fields: fields}} = query, sources, params) do
    distinct = distinct(query.distinct, query)
    ["SELECT ", distinct | select_fields(fields, sources, params, query)]
  end

  defp select_fields([], _sources, _params, _query), do: "'TRUE'"

  defp select_fields(fields, sources, params, query) do
    intersperse_map(fields, ",", fn
      {k, v} -> [expr(v, sources, params, query), " AS " | quote_name(k)]
      v -> expr(v, sources, params, query)
    end)
  end

  defp distinct(nil, _query), do: []
  defp distinct(%QueryExpr{expr: []}, _query), do: []
  defp distinct(%QueryExpr{expr: true}, _query), do: "DISTINCT "
  defp distinct(%QueryExpr{expr: false}, _query), do: []

  defp distinct(%QueryExpr{}, query) do
    error!(
      query,
      "DISTINCT ON is not supported! Use `distinct: true`, for ex. `from rec in MyModel, distinct: true, select: rec.my_field`"
    )
  end

  defp from(%Query{from: %{source: source, hints: hints}} = query, sources, params) do
    {from, name} = get_source(query, sources, params, 0, source)
    [" FROM ", from, " AS ", name | hints(hints)]
  end

  defp join(%Query{joins: []}, _sources, _params), do: []

  defp join(%Query{joins: joins} = query, sources, params) do
    [
      ?\s
      | intersperse_map(joins, ?\s, fn
          %JoinExpr{qual: qual, ix: ix, source: source, on: %QueryExpr{expr: on_exrp}} ->
            {join, name} = get_source(query, sources, params, ix, source)
            [join_qual(qual), join, " AS ", name, on_join_expr(on_exrp)]
        end)
    ]
  end

  defp on_join_expr({_, _, [h | t]}) do
    [on_join_expr(h) | on_join_expr(t)]
    |> Enum.uniq()
    |> Enum.join(",")
  end

  defp on_join_expr([h | t]) do
    [on_join_expr(h) | t]
  end

  defp on_join_expr({{:., [], [{:&, [], _}, col]}, [], []}) when is_atom(col) do
    " USING " <> Atom.to_string(col)
  end

  defp on_join_expr({:==, _, [{{_, _, [_, col]}, _, _}, _]}) when is_atom(col) do
    " USING " <> Atom.to_string(col)
  end

  defp on_join_expr(true), do: ""

  defp join_qual(:inner), do: " INNER JOIN "
  defp join_qual(:inner_lateral), do: " ARRAY JOIN "
  # TODO can it be this way? is yes, "deprecate" inner_lateral and left_lateral
  defp join_qual(:array), do: " ARRAY JOIN "
  defp join_qual(:cross), do: " CROSS JOIN "
  defp join_qual(:full), do: " FULL JOIN "
  defp join_qual(:left_lateral), do: " LEFT ARRAY JOIN "
  # TODO can it be this way?
  defp join_qual(:left_array), do: " LEFT ARRAY JOIN "
  defp join_qual(:left), do: " LEFT OUTER JOIN "

  defp where(%Query{wheres: wheres} = query, sources, params) do
    boolean(" WHERE ", wheres, sources, params, query)
  end

  defp having(%Query{havings: havings} = query, sources, params) do
    boolean(" HAVING ", havings, sources, params, query)
  end

  defp group_by(%Query{group_bys: []}, _sources, _params), do: []

  defp group_by(%Query{group_bys: group_bys} = query, sources, params) do
    [
      " GROUP BY "
      | intersperse_map(group_bys, ", ", fn %QueryExpr{expr: expr} ->
          intersperse_map(expr, ", ", &expr(&1, sources, params, query))
        end)
    ]
  end

  defp order_by(%Query{order_bys: []}, _sources, _params), do: []

  defp order_by(%Query{order_bys: order_bys} = query, sources, params) do
    order_bys = Enum.flat_map(order_bys, & &1.expr)
    [" ORDER BY " | intersperse_map(order_bys, ", ", &order_by_expr(&1, sources, params, query))]
  end

  @dialyzer {:no_improper_lists, order_by_expr: 4}
  defp order_by_expr({dir, expr}, sources, params, query) do
    str = expr(expr, sources, params, query)

    case dir do
      :asc -> str
      :desc -> [str | " DESC"]
    end
  end

  defp limit(%Query{limit: nil}, _sources, _params), do: []

  defp limit(%Query{limit: %QueryExpr{expr: expr}} = query, sources, params) do
    [" LIMIT ", expr(expr, sources, params, query)]
  end

  defp offset(%Query{offset: nil}, _sources, _params), do: []

  defp offset(%Query{offset: %QueryExpr{expr: expr}} = query, sources, params) do
    [" OFFSET ", expr(expr, sources, params, query)]
  end

  defp hints([_ | _] = hints) do
    [" " | intersperse_map(hints, ", ", &hint/1)]
  end

  defp hints([]), do: []

  defp hint(hint) when is_binary(hint), do: hint

  defp hint({k, v}) when is_atom(k) and is_integer(v),
    do: [Atom.to_string(k), " ", Integer.to_string(v)]

  defp boolean(_name, [], _sources, _params, _query), do: []

  defp boolean(name, [%{expr: expr, op: op} | exprs], sources, params, query) do
    {_, op} =
      Enum.reduce(exprs, {op, paren_expr(expr, sources, params, query)}, fn
        %BooleanExpr{expr: expr, op: op}, {op, acc} ->
          {op, [acc, operator_to_boolean(op), paren_expr(expr, sources, params, query)]}

        %BooleanExpr{expr: expr, op: op}, {_, acc} ->
          {op, [?(, acc, ?), operator_to_boolean(op), paren_expr(expr, sources, params, query)]}
      end)

    [name | op]
  end

  defp operator_to_boolean(:and), do: " AND "
  defp operator_to_boolean(:or), do: " OR "

  defp paren_expr(false, _sources, _params, _query), do: "(0=1)"
  defp paren_expr(true, _sources, _params, _query), do: "(1=1)"

  defp paren_expr(expr, sources, params, query) do
    [?(, expr(expr, sources, params, query), ?)]
  end

  @dialyzer {:no_improper_lists, expr: 4}
  defp expr({_type, [literal]}, sources, params, query) do
    expr(literal, sources, params, query)
  end

  defp expr({:^, [], [ix]}, _sources, params, _query) do
    ["{$", Integer.to_string(ix), ?:, param_type_at(params, ix), ?}]
  end

  defp expr({:^, [], [ix, _]}, _sources, params, _query) do
    ["{$", Integer.to_string(ix), ?:, param_type_at(params, ix), ?}]
  end

  defp expr({{:., _, [{:&, _, [idx]}, field]}, _, []}, sources, _params, _query)
       when is_atom(field) do
    quote_qualified_name(field, sources, idx)
  end

  defp expr({:&, _, [idx, fields, _counter]}, sources, _params, query) do
    {_, name, schema} = elem(sources, idx)

    if is_nil(schema) and is_nil(fields) do
      error!(
        query,
        "ClickHouse requires a schema module when using selector " <>
          "#{inspect(name)} but none was given. " <>
          "Please specify a schema or specify exactly which fields from " <>
          "#{inspect(name)} you desire"
      )
    end

    intersperse_map(fields, ", ", &[name, ?. | quote_name(&1)])
  end

  defp expr({:in, _, [_left, []]}, _sources, _params, _query), do: "0"

  defp expr({:in, _, [left, right]}, sources, params, query) when is_list(right) do
    args = intersperse_map(right, ?,, &expr(&1, sources, params, query))
    [expr(left, sources, params, query), " IN (", args, ?)]
  end

  defp expr({:in, _, [_, {:^, _, [_, 0]}]}, _sources, _params, _query), do: "0"

  defp expr({:in, _, [left, %Tagged{value: {:^, [], [ix]}, type: type}]}, sources, params, query) do
    [
      expr(left, sources, params, query),
      " IN {$",
      Integer.to_string(ix),
      ?:,
      tagged_to_db(type),
      ?}
    ]
  end

  defp expr({:in, _, [left, {:^, [], [ix]}]}, sources, params, query) do
    [
      expr(left, sources, params, query),
      " IN {$",
      Integer.to_string(ix),
      ?:,
      param_type_at(params, ix),
      ?}
    ]
  end

  defp expr({:in, _, [left, right]}, sources, params, query) do
    [expr(left, sources, params, query), " IN ", expr(right, sources, params, query)]
  end

  defp expr({:is_nil, _, [arg]}, sources, params, query) do
    [expr(arg, sources, params, query) | " IS NULL"]
  end

  defp expr({:not, _, [expr]}, sources, params, query) do
    case expr do
      {fun, _, _} when fun in @binary_ops -> ["NOT (", expr(expr, sources, params, query), ?)]
      _ -> ["~(", expr(expr, sources, params, query), ?)]
    end
  end

  defp expr(%SubQuery{query: query, params: _}, _sources, params, _query) do
    all(query, params)
  end

  defp expr({:fragment, _, [kw]}, sources, params, query)
       when is_list(kw) or tuple_size(kw) == 3 do
    Enum.reduce(kw, query, fn {k, {op, v}}, query ->
      # TODO what's that?
      expr({op, nil, [k, v]}, sources, params, query)
    end)
  end

  defp expr({:fragment, _, parts}, sources, params, query) do
    Enum.map(parts, fn
      {:raw, part} -> part
      {:expr, expr} -> expr(expr, sources, params, query)
    end)
  end

  defp expr({fun, _, args}, sources, params, query) when is_atom(fun) and is_list(args) do
    {modifier, args} =
      case args do
        [rest, :distinct] -> {"DISTINCT ", [rest]}
        _ -> {[], args}
      end

    case handle_call(fun, length(args)) do
      {:binary_op, op} ->
        [left, right] = args

        [
          op_to_binary(left, sources, params, query),
          op | op_to_binary(right, sources, params, query)
        ]

      {:fun, fun} ->
        [fun, ?(, modifier, intersperse_map(args, ", ", &expr(&1, sources, params, query)), ?)]
    end
  end

  defp expr({:count, _, []}, _sources, _params, _query), do: "count(*)"

  defp expr(list, sources, params, query) when is_list(list) do
    ["ARRAY[", intersperse_map(list, ?,, &expr(&1, sources, params, query)), ?]]
  end

  defp expr(%Decimal{} = decimal, _sources, _params, _query) do
    Decimal.to_string(decimal, :normal)
  end

  # TOOD needed?
  defp expr(%Tagged{value: binary, type: :binary}, _sources, _params, _query)
       when is_binary(binary) do
    # TODO silence warning
    ["0x" | Base.encode16(binary, case: :lower)]
  end

  defp expr(%Tagged{value: {:^, [], [ix]}, type: type}, _sources, _params, _query) do
    ["{$", Integer.to_string(ix), ?:, tagged_to_db(type), ?}]
  end

  defp expr(%Tagged{value: value, type: type}, sources, params, query) do
    ["CAST(", expr(value, sources, params, query), " AS ", tagged_to_db(type), ?)]
  end

  defp expr(nil, _sources, _params, _query), do: "NULL"
  defp expr(true, _sources, _params, _query), do: "1"
  defp expr(false, _sources, _params, _query), do: "0"

  defp expr(literal, _sources, _params, _query) when is_binary(literal) do
    [?\', escape_string(literal), ?\']
  end

  defp expr(literal, _sources, _params, _query) when is_integer(literal),
    do: Integer.to_string(literal)

  defp expr(literal, _sources, _params, _query) when is_float(literal),
    do: Float.to_string(literal)

  defp expr(literal, _sources, _params, _query) when is_atom(literal), do: Atom.to_string(literal)

  # defp interal(count, _interval, sources, query) do
  #   [expr(count, sources, query)]
  # end

  defp op_to_binary({op, _, [_, _]} = expr, sources, params, query) when op in @binary_ops do
    paren_expr(expr, sources, params, query)
  end

  defp op_to_binary(expr, sources, params, query) do
    expr(expr, sources, params, query)
  end

  defp create_names(%{sources: sources}) do
    sources |> create_names(0, tuple_size(sources)) |> List.to_tuple()
  end

  defp create_names(sources, pos, size) when pos < size do
    [create_name(sources, pos) | create_names(sources, pos + 1, size)]
  end

  defp create_names(_sources, size, size), do: []

  @dialyzer {:no_improper_lists, create_name: 2}
  defp create_name(sources, pos) do
    case elem(sources, pos) do
      {:fragment, _, _} ->
        {nil, [?f | Integer.to_string(pos)], nil}

      {table, schema, prefix} ->
        name = [create_alias(table) | Integer.to_string(pos)]
        {quote_table(prefix, table), name, schema}

      %SubQuery{} ->
        {nil, [?s | Integer.to_string(pos)], nil}
    end
  end

  # TODO wow, two whens
  defp create_alias(<<first, _rest::bytes>>) when first in ?a..?z when first in ?A..?Z do
    <<first>>
  end

  defp create_alias(_), do: ?t

  @doc false
  def intersperse_map([elem], _separator, mapper), do: [mapper.(elem)]

  def intersperse_map([elem | rest], separator, mapper) do
    [mapper.(elem), separator | intersperse_map(rest, separator, mapper)]
  end

  def intersperse_map([], _separator, _mapper), do: []

  @doc false
  def quote_name(name, quoter \\ ?")
  def quote_name(nil, _), do: []

  def quote_name(names, quoter) when is_list(names) do
    names
    |> Enum.reject(&is_nil/1)
    |> intersperse_map(?., &quote_name(&1, nil))
    |> wrap_in(quoter)
  end

  def quote_name(name, quoter) when is_atom(name) do
    name |> Atom.to_string() |> quote_name(quoter)
  end

  def quote_name(name, quoter) do
    if String.contains?(name, <<quoter>>) do
      error!(nil, "bad name #{inspect(name)}")
    end

    wrap_in(name, quoter)
  end

  defp quote_qualified_name(name, sources, ix) do
    {_, source, _} = elem(sources, ix)
    [source, ?. | quote_name(name)]
  end

  @doc false
  def quote_table(prefix, name)
  def quote_table(nil, name), do: quote_name(name)
  def quote_table(prefix, name), do: [quote_name(prefix), ?., quote_name(name)]

  defp wrap_in(value, nil), do: value
  # defp wrap_in(value, {left, right}), do: [left, value, right]
  defp wrap_in(value, wrapper), do: [wrapper, value, wrapper]

  defp escape_string(value) when is_binary(value) do
    :binary.replace(value, "'", "''", [:global])
  end

  defp error!(nil, message) do
    raise ArgumentError, message
  end

  defp error!(query, message) do
    raise Ecto.QueryError, query: query, message: message
  end

  defp get_source(query, sources, params, ix, source) do
    {expr, name, _schema} = elem(sources, ix)
    {expr || paren_expr(source, sources, params, query), name}
  end

  defp tagged_to_db(:integer), do: "Int64"
  defp tagged_to_db(other), do: ecto_to_db(other)

  # TODO
  defp ecto_to_db({:array, t}), do: "Array(#{ecto_to_db(t)})"

  # TODO
  defp ecto_to_db({:nested, types}) do
    fields =
      Tuple.to_list(types)
      |> Enum.map(fn {field, type} ->
        Atom.to_string(field) <> " " <> ecto_to_db(type)
      end)
      |> Enum.join(", ")

    "Nested(#{fields})"
  end

  # TODO
  defp ecto_to_db(:id), do: "UInt32"
  # TODO
  defp ecto_to_db(:binary_id), do: "FixedString(36)"
  # TODO
  defp ecto_to_db(:uuid), do: "FixedString(36)"
  defp ecto_to_db(:string), do: "String"
  # TODO
  defp ecto_to_db(:binary), do: "FixedString(4000)"
  # TODO
  defp ecto_to_db(:integer), do: "Int32"
  defp ecto_to_db(:bigint), do: "Int64"
  # TODO
  defp ecto_to_db(:float), do: "Float32"
  defp ecto_to_db(:decimal), do: "Float64"
  defp ecto_to_db(:boolean), do: "UInt8"
  defp ecto_to_db(:date), do: "Date"
  defp ecto_to_db(:utc_datetime), do: "DateTime"
  defp ecto_to_db(:naive_datetime), do: "DateTime"
  defp ecto_to_db(:timestamp), do: "DateTime"
  defp ecto_to_db(other), do: Atom.to_string(other)

  defp param_type_at(params, ix) do
    value = Enum.at(params, ix)
    ch_typeof(value)
  end

  defp ch_typeof(s) when is_binary(s), do: "String"
  defp ch_typeof(i) when is_integer(i), do: "Int64"
  defp ch_typeof(f) when is_float(f), do: "Float64"
  defp ch_typeof(%DateTime{}), do: "DateTime"
  defp ch_typeof(%Date{}), do: "Date"
  defp ch_typeof(%NaiveDateTime{}), do: "DateTime"
  defp ch_typeof([v | _]), do: ["Array(", ch_typeof(v), ?)]
end
