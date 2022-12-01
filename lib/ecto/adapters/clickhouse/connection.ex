defmodule Ecto.Adapters.ClickHouse.Connection do
  @moduledoc false
  alias Ecto.{Query, SubQuery}
  alias Ecto.Query.{QueryExpr, JoinExpr, BooleanExpr, Tagged}

  @spec all(Ecto.Query.t()) :: iodata
  def all(query) do
    sources = create_names(query)
    # TODO there is no order_by_distinct
    {select_distinct, order_by_distinct} = distinct(query.distinct, sources, query)
    from = from(query, sources)
    select = select(query, select_distinct, sources)
    join = join(query, sources)
    where = where(query, sources)
    group_by = group_by(query, sources)
    having = having(query, sources)
    order_by = order_by(query, order_by_distinct, sources)
    limit = limit(query, sources)
    offset = offset(query, sources)
    [select, from, join, where, group_by, having, order_by, limit, offset]
  end

  def delete(_prefix, _table, _filters, _returning) do
    raise "not implemented"
  end

  def delete_all(_query) do
    raise "not implemented"
  end

  def insert(_prefix, _table, _header, _rows, _on_conflict, _returning, _placeholder) do
    # TODO note that insert_stream can be used instead?
    raise "not implemented"
  end

  def update(_prefix, _table, _fields, _filters, _returning) do
    raise "not implemented"
  end

  def update_all(_query) do
    raise "not implemented"
  end

  def stream(_conn, _prepared, _params, _opts) do
    raise "not implemented"
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

  defp select(%Query{select: %{fields: fields}} = query, select_distinct, sources) do
    ["SELECT", select_distinct, ?\s | select_fields(fields, sources, query)]
  end

  defp select_fields([], _sources, _query), do: "'TRUE'"

  defp select_fields(fields, sources, query) do
    intersperce_map(fields, ",", fn
      {k, v} -> [expr(v, sources, query), " AS " | quote_name(k)]
      v -> expr(v, sources, query)
    end)
  end

  # TODO source are not used in any clause
  defp distinct(nil, _source, _query), do: {[], []}
  defp distinct(%QueryExpr{expr: []}, _sources, _query), do: {[], []}
  # TODO can have space after?
  defp distinct(%QueryExpr{expr: true}, _sources, _query), do: {" DISTINCT", []}
  defp distinct(%QueryExpr{expr: false}, _sources, _query), do: {[], []}

  defp distinct(%QueryExpr{}, _sources, query) do
    error!(
      query,
      "DISTINCT ON is not supported! Use `distinct: true`, for ex. `from rec in MyModel, distinct: true, select: rec.my_field`"
    )
  end

  defp from(%Query{from: %{source: source, hints: hints}} = query, sources) do
    {from, name} = get_source(query, sources, 0, source)
    [" FROM ", from, " AS ", name | hints(hints)]
  end

  defp join(%Query{joins: []}, _sources), do: []

  defp join(%Query{joins: joins} = query, sources) do
    [
      ?\s
      | intersperce_map(joins, ?\s, fn
          %JoinExpr{qual: qual, ix: ix, source: source, on: %QueryExpr{expr: on_exrp}} ->
            {join, name} = get_source(query, sources, ix, source)
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

  defp where(%Query{wheres: wheres} = query, sources) do
    boolean(" WHERE ", wheres, sources, query)
  end

  defp having(%Query{havings: havings} = query, sources) do
    boolean(" HAVING ", havings, sources, query)
  end

  defp group_by(%Query{group_bys: []}, _sources), do: []

  defp group_by(%Query{group_bys: group_bys} = query, sources) do
    [
      " GROUP UP "
      | intersperce_map(group_bys, ", ", fn %QueryExpr{expr: expr} ->
          intersperce_map(expr, ", ", &expr(&1, sources, query))
        end)
    ]
  end

  defp order_by(%Query{order_bys: []}, _distinct, _souces), do: []

  # TODO distinct is always []
  defp order_by(%Query{order_bys: order_bys} = query, distinct, sources) do
    order_bys = Enum.flat_map(order_bys, & &1.expr)

    [
      " ORDER BY "
      | intersperce_map(distinct ++ order_bys, ", ", &order_by_expr(&1, sources, query))
    ]
  end

  defp order_by_expr({dir, expr}, sources, query) do
    str = expr(expr, sources, query)

    case dir do
      :asc -> str
      :desc -> [str | " DESC"]
    end
  end

  defp limit(%Query{limit: nil}, _sources), do: []

  defp limit(%Query{limit: %QueryExpr{expr: expr}} = query, sources) do
    [" LIMIT ", expr(expr, sources, query)]
  end

  defp offset(%Query{offset: nil}, _sources), do: []

  defp offset(%Query{offset: %QueryExpr{expr: expr}} = query, sources) do
    [" OFFSET ", expr(expr, sources, query)]
  end

  defp hints([_ | _] = hints) do
    [" " | intersperce_map(hints, ", ", &hint/1)]
  end

  defp hints([]), do: []

  defp hint(hint) when is_binary(hint), do: hint

  defp hint({k, v}) when is_atom(k) and is_integer(v),
    do: [Atom.to_string(k), " ", Integer.to_string(v)]

  defp boolean(_name, [], _sources, _query), do: []

  defp boolean(name, [%{expr: expr, op: op} | exprs], sources, query) do
    {_, op} =
      exprs
      |> Enum.reduce({op, paren_expr(expr, sources, query)}, fn
        %BooleanExpr{expr: expr, op: op}, {op, acc} ->
          {op, [acc, operator_to_boolean(op), paren_expr(expr, sources, query)]}

        %BooleanExpr{expr: expr, op: op}, {_, acc} ->
          {op, [?(, acc, ?), operator_to_boolean(op), paren_expr(expr, sources, query)]}
      end)

    [name | op]
  end

  defp operator_to_boolean(:and), do: " AND "
  defp operator_to_boolean(:or), do: " OR "

  defp paren_expr(false, _sources, _query), do: "(0=1)"
  defp paren_expr(true, _sources, _query), do: "(1=1)"

  defp paren_expr(expr, sources, query) do
    [?(, expr(expr, sources, query), ?)]
  end

  defp expr({_type, [literal]}, sources, query) do
    expr(literal, sources, query)
  end

  defp expr({:^, [], [_ix]}, _sources, _query) do
    # TODO params
    [??]
  end

  defp expr({{:., _, [{:&, _, [idx]}, field]}, _, []}, sources, _query) when is_atom(field) do
    quote_qualified_name(field, sources, idx)
  end

  defp expr({:&, _, [idx, fields, _counter]}, sources, query) do
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

    intersperce_map(fields, ", ", &[name, ?. | quote_name(&1)])
  end

  defp expr({:in, _, [_left, []]}, _sources, _query), do: "0"

  defp expr({:in, _, [left, right]}, sources, query) when is_list(right) do
    args = intersperce_map(right, ?,, &expr(&1, sources, query))
    [expr(left, sources, query), " IN (", args, ?)]
  end

  defp expr({:in, _, [_, {:^, _, [_, 0]}]}, _sources, _query), do: "0"

  defp expr({:in, _, [left, {:^, _, [_, length]}]}, sources, query) do
    # TODO params
    args = Enum.intersperse(List.duplicate(??, length), ?,)
    [expr(left, sources, query), " IN(", args, ?)]
  end

  defp expr({:in, _, [left, right]}, sources, query) do
    [expr(left, sources, query), " =ANY(", expr(right, sources, query), ?)]
  end

  defp expr({:is_nil, _, [arg]}, sources, query) do
    [expr(arg, sources, query) | " IS NULL"]
  end

  defp expr({:not, _, [expr]}, sources, query) do
    case expr do
      {fun, _, _} when fun in @binary_ops -> ["NOT (", expr(expr, sources, query), ?)]
      _ -> ["~(", expr(expr, sources, query), ?)]
    end
  end

  # TODO params?
  defp expr(%SubQuery{query: query, params: _}, _sources, _query) do
    all(query)
  end

  defp expr({:fragment, _, [kw]}, sources, query) when is_list(kw) or tuple_size(kw) == 3 do
    Enum.reduce(kw, query, fn {k, {op, v}}, query ->
      # TODO
      expr({op, nil, [k, v]}, sources, query)
    end)
  end

  defp expr({:fragment, _, parts}, sources, query) do
    Enum.map(parts, fn
      # TODO
      {:raw, part} -> part
      {:expr, expr} -> expr(expr, sources, query)
    end)
  end

  defp expr({fun, _, args}, sources, query) when is_atom(fun) and is_list(args) do
    {modifier, args} =
      case args do
        [rest, :distinct] -> {"DISTINCT ", [rest]}
        _ -> {[], args}
      end

    case handle_call(fun, length(args)) do
      {:binary_op, op} ->
        [left, right] = args
        [op_to_binary(left, sources, query), op | op_to_binary(right, sources, query)]

      {:fun, fun} ->
        [fun, ?(, modifier, intersperce_map(args, ", ", &expr(&1, sources, query)), ?)]
    end
  end

  defp expr({:count, _, []}, _sources, _query), do: "count(*)"

  defp expr(list, sources, query) when is_list(list) do
    ["ARRAY[", intersperce_map(list, ?,, &expr(&1, sources, query)), ?]]
  end

  defp expr(%Decimal{} = decimal, _sources, _query) do
    Decimal.to_string(decimal, :normal)
  end

  defp expr(%Tagged{value: binary, type: :binary}, _sources, _query)
       when is_binary(binary) do
    ["0x" | Base.encode16(binary, case: :lower)]
  end

  defp expr(%Tagged{value: {:^, [], [0]}, type: type}, _sources, _query) do
    ["{var:", tagged_to_db(type), ?}]
  end

  defp expr(%Tagged{value: value, type: type}, sources, query) do
    ["CAST(", expr(value, sources, query), " AS ", tagged_to_db(type), ?)]
  end

  defp expr(nil, _sources, _query), do: "NULL"
  defp expr(true, _sources, _query), do: "1"
  defp expr(false, _sources, _query), do: "0"

  defp expr(literal, _sources, _query) when is_binary(literal) do
    [?\', escape_string(literal), ?\']
  end

  defp expr(literal, _sources, _query) when is_integer(literal), do: Integer.to_string(literal)
  defp expr(literal, _sources, _query) when is_float(literal), do: Float.to_string(literal)
  defp expr(literal, _sources, _query) when is_atom(literal), do: Atom.to_string(literal)

  # defp interal(count, _interval, sources, query) do
  #   [expr(count, sources, query)]
  # end

  defp op_to_binary({op, _, [_, _]} = expr, sources, query) when op in @binary_ops do
    paren_expr(expr, sources, query)
  end

  defp op_to_binary(expr, sources, query) do
    expr(expr, sources, query)
  end

  defp create_names(%{sources: sources}) do
    sources |> create_names(0, tuple_size(sources)) |> List.to_tuple()
  end

  defp create_names(sources, pos, size) when pos < size do
    [create_name(sources, pos) | create_names(sources, pos + 1, size)]
  end

  defp create_names(_sources, size, size), do: []

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

  defp intersperce_map([elem], _separator, mapper), do: [mapper.(elem)]

  defp intersperce_map([elem | rest], separator, mapper) do
    [mapper.(elem), separator | intersperce_map(rest, separator, mapper)]
  end

  defp intersperce_map([], _separator, _mapper), do: []

  defp quote_name(name, quoter \\ ?")
  defp quote_name(nil, _), do: []

  defp quote_name(names, quoter) when is_list(names) do
    names
    |> Enum.reject(&is_nil/1)
    |> intersperce_map(?., &quote_name(&1, nil))
    |> wrap_in(quoter)
  end

  defp quote_name(name, quoter) when is_atom(name) do
    name |> Atom.to_string() |> quote_name(quoter)
  end

  defp quote_name(name, quoter) do
    if String.contains?(name, <<quoter>>) do
      error!(nil, "bad name #{inspect(name)}")
    end

    wrap_in(name, quoter)
  end

  defp quote_qualified_name(name, sources, ix) do
    {_, source, _} = elem(sources, ix)
    [source, ?. | quote_name(name)]
  end

  defp quote_table(prefix, name)
  defp quote_table(nil, name), do: quote_name(name)
  defp quote_table(prefix, name), do: [quote_name(prefix), ?., quote_name(name)]

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

  defp get_source(query, sources, ix, source) do
    {expr, name, _schema} = elem(sources, ix)
    {expr || paren_expr(source, sources, query), name}
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
  defp ecto_to_db(:binary_id), do: "FixedString(36)"
  defp ecto_to_db(:uuid), do: "FixedString(36)"
  defp ecto_to_db(:string), do: "String"
  # TODO
  defp ecto_to_db(:binary), do: "FixedString(4000)"
  defp ecto_to_db(:integer), do: "Int32"
  defp ecto_to_db(:bigint), do: "Int64"
  defp ecto_to_db(:float), do: "Float32"
  defp ecto_to_db(:decimal), do: "Float64"
  defp ecto_to_db(:boolean), do: "UInt8"
  defp ecto_to_db(:date), do: "Date"
  defp ecto_to_db(:utc_datetime), do: "DateTime"
  defp ecto_to_db(:naive_datetime), do: "DateTime"
  defp ecto_to_db(:timestamp), do: "DateTime"
  defp ecto_to_db(other), do: Atom.to_string(other)
end
