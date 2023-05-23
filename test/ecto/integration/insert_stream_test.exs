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
  end

  defp naive_now do
    NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)
  end
end
