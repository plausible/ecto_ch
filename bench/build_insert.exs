# measures the time and memory required to build an insert statement

row = [
  1,
  "1",
  ~N[2022-11-26 09:38:24],
  ~N[2022-11-26 09:38:25],
  ["here", "goes", "the", "string"],
  ["oh, no", "it's", "an", "array"]
]

types = [:u32, :string, :datetime, :datetime, {:array, :string}, {:array, :string}]
header = [:a, :b, :c, :d, :e, :f]

input = fn n ->
  rows = List.duplicate(row, n)
  # in insert_all ClickhouseEcto wants maps or keyword lists
  keyed = Enum.map(rows, fn row -> Enum.zip(header, row) end)
  {rows, keyed}
end

Benchee.run(
  %{
    "chto" => fn {rows, _} -> Bench.BuildInsert.chto(rows, types: types, fields: header) end,
    "clickhouse_ecto" => fn {_, rows} -> Bench.BuildInsert.clickhouse_ecto(rows, header) end
  },
  memory_time: 2,
  inputs: %{
    "small (50)" => input.(50),
    "medium (500)" => input.(500),
    "big (5000)" => input.(5000),
    "very big (50000)" => input.(50000),
    "very very big (500_000)" => input.(500_000)
  }
)
