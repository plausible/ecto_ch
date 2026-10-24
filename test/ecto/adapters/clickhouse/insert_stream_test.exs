defmodule Ecto.Adapters.ClickHouse.InsertStreamTest do
  use Ecto.Integration.Case

  alias Ecto.Integration.TestRepo
  alias EctoClickHouse.Integration.Account

  defmodule AliasedRow do
    use Ecto.Schema

    @primary_key false
    schema "streamed_alias_rows" do
      field :id, Ch, type: "UInt64"
      field :label, :string, source: :stored_label
    end
  end

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

    test "preserves map and keyword rows across partial chunks" do
      timestamp = ~N[2020-01-01 00:00:00]

      rows =
        Stream.map(1..5, fn id ->
          row = [name: "row-#{id}", updated_at: timestamp, id: id, inserted_at: timestamp]
          if rem(id, 2) == 0, do: row, else: Map.new(row)
        end)

      assert {5, nil} = TestRepo.insert_stream(Account, rows, chunk_every: 2)

      assert TestRepo.query!("SELECT id, name FROM accounts ORDER BY id").rows ==
               for(id <- 1..5, do: [id, "row-#{id}"])
    end

    test "empty schema and schemaless streams do not insert rows" do
      rows = Stream.map([], & &1)

      assert {0, nil} = TestRepo.insert_stream(Account, rows, chunk_every: 2)

      assert {0, nil} =
               TestRepo.insert_stream("accounts", rows,
                 types: [id: :u64, name: :string, inserted_at: :datetime, updated_at: :datetime],
                 chunk_every: 2
               )

      assert TestRepo.query!("SELECT count() FROM accounts").rows == [[0]]
    end

    test "a table and schema tuple inserts into the overridden table" do
      TestRepo.query!("""
      CREATE TABLE streamed_accounts (
        id UInt64,
        name String,
        inserted_at DateTime,
        updated_at DateTime
      ) ENGINE Memory
      """)

      on_exit(fn -> TestRepo.query!("DROP TABLE streamed_accounts") end)
      timestamp = ~N[2020-01-01 00:00:00]
      rows = [%{id: 17, name: "overridden", inserted_at: timestamp, updated_at: timestamp}]

      assert {1, nil} = TestRepo.insert_stream({"streamed_accounts", Account}, rows)

      assert TestRepo.query!("SELECT id, name FROM streamed_accounts").rows ==
               [[17, "overridden"]]

      assert TestRepo.query!("SELECT count() FROM accounts").rows == [[0]]
    end

    test "schema field sources identify columns while rows use schema field names" do
      TestRepo.query!("""
      CREATE TABLE streamed_alias_rows (id UInt64, stored_label String) ENGINE Memory
      """)

      on_exit(fn -> TestRepo.query!("DROP TABLE streamed_alias_rows") end)
      rows = [%{id: 1, label: "first"}, [label: "second", id: 2]]

      assert {2, nil} = TestRepo.insert_stream(AliasedRow, rows, chunk_every: 1)

      assert TestRepo.query!("SELECT id, stored_label FROM streamed_alias_rows ORDER BY id").rows ==
               [[1, "first"], [2, "second"]]
    end
  end

  defp naive_now do
    NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)
  end
end
