# measures the time and memory required to build an insert statement

rows =
  List.duplicate(
    [
      1,
      "1",
      ~N[2022-11-26 09:38:24],
      ~N[2022-11-26 09:38:25],
      ["here", "goes", "the", "string"],
      ["oh, no", "it's", "an", "array"]
    ],
    300
  )

types = [:u32, :string, :datetime, :datetime, {:array, :string}, {:array, :string}]
header = [:a, :b, :c, :d, :e, :f]

# in insert_all ClickhouseEcto wants maps or keyword lists
maps = Enum.map(rows, fn row -> Enum.zip(header, row) end)

Benchee.run(
  %{
    "chto" => fn -> Bench.BuildInsert.chto(rows, types: types, fields: header) end,
    "clickhouse_ecto" => fn -> Bench.BuildInsert.clickhouse_ecto(maps, header) end
  },
  memory_time: 2
)
