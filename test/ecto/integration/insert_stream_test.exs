defmodule Ecto.Integration.InsertStreamTest do
  use Ecto.Integration.Case

  alias Ecto.Integration.TestRepo
  alias EctoClickHouse.Integration.Account

  describe "insert_stream/2" do
    test "with schema" do
      accounts =
        Stream.map(1..10000, fn i ->
          %{id: i, name: "John-#{i}", inserted_at: naive_now(), updated_at: naive_now()}
        end)

      assert {10000, nil} = TestRepo.insert_stream(Account, accounts)
    end

    test "with table" do
      accounts =
        Stream.map(1..10000, fn i ->
          [id: i, name: "John-#{i}", inserted_at: naive_now(), updated_at: naive_now()]
        end)

      assert {10000, nil} =
               TestRepo.insert_stream("accounts", accounts,
                 types: [id: :u64, name: :string, inserted_at: :datetime, updated_at: :datetime]
               )
    end

    test "preserves named tuple headers" do
      TestRepo.query!("""
      CREATE TABLE named_tuple_stream (
        id UInt64,
        value Tuple(name LowCardinality(String), count UInt64),
        values Array(Tuple(name String, count UInt64))
      ) ENGINE Memory
      """)

      on_exit(fn -> TestRepo.query!("DROP TABLE named_tuple_stream") end)

      rows = [
        [1, {"first", 42}, [{"nested", 7}, {"another", 8}]],
        [2, {"second", 0}, []]
      ]

      types = [
        id: "UInt64",
        value: "Tuple(name LowCardinality(String), count UInt64)",
        values: "Array(Tuple(name String, count UInt64))"
      ]

      entries =
        Stream.map(rows, fn [id, value, values] -> [id: id, value: value, values: values] end)

      assert {2, nil} =
               TestRepo.insert_stream("named_tuple_stream", entries, types: types, chunk_every: 1)

      assert TestRepo.query!("SELECT id, value, values FROM named_tuple_stream ORDER BY id").rows ==
               rows
    end
  end

  defp naive_now do
    NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)
  end
end
