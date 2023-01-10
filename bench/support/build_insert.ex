defmodule Bench.BuildInsert do
  # TODO bench stream impl as well
  @doc """

      iex> Bench.BuildInsert.chto([[1, 2]], types: [:u8, :u8], fields: [:a, :b])
      [
        [
          "INSERT INTO ",
          [34, "example", 34],
          40,
          [[34, "a", 34], 44, [34, "b", 34]],
          41
        ],
        " FORMAT ",
        "RowBinary",
        10,
        [<<1>>, <<2>>]
      ]

  """
  def chto(rows, opts) do
    {statement, opts} = Ecto.Adapters.ClickHouse.build_insert("example", opts)
    types = opts[:types] || raise "missing :types for #{inspect(statement)}"
    format = opts[:format] || "RowBinary"
    rows = Ch.RowBinary.encode_rows(rows, types)
    [statement, " FORMAT ", format, ?\n | rows]
  end

  @doc """

      iex. Bench.BuildInsert.clickhouse_ecto([[a: 1, b: 2]], [:a, :b])
      %Clickhousex.HTTPRequest{
        post_data: [" (", "1", ",", "2", ")"],
        query_string_data: "INSERT INTO \"example\" (\"a\",\"b\")  FORMAT Values"
      }

  """
  def clickhouse_ecto(rows, header) do
    # Ecto.Adapters.SQL.insert_all, but without query call
    {rows, params} =
      case rows do
        {%Ecto.Query{} = query, params} -> {query, Enum.reverse(params)}
        rows -> unzip_inserts(header, rows)
      end

    all_params = Enum.reverse(params)

    statement =
      ClickhouseEcto.Connection.insert(
        _prefix = nil,
        "example",
        header,
        rows,
        _on_conflict = nil,
        _returning = [],
        _placeholders = []
      )

    query = DBConnection.Query.parse(%Clickhousex.Query{statement: statement}, [])
    DBConnection.Query.encode(query, all_params, [])
  end

  defp unzip_inserts(header, rows) do
    Enum.map_reduce(rows, [], fn fields, params ->
      Enum.map_reduce(header, params, fn key, acc ->
        case :lists.keyfind(key, 1, fields) do
          {^key, {%Ecto.Query{} = query, query_params}} ->
            {{query, length(query_params)}, Enum.reverse(query_params, acc)}

          {^key, {:placeholder, placeholder_index}} ->
            {{:placeholder, Integer.to_string(placeholder_index)}, acc}

          {^key, value} ->
            {key, [value | acc]}

          false ->
            {nil, acc}
        end
      end)
    end)
  end
end
