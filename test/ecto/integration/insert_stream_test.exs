defmodule Ecto.Integration.InsertStreamTest do
  use Ecto.Integration.Case

  alias Ecto.Integration.TestRepo
  alias EctoClickHouse.Integration.Account

  describe "insert into stream" do
    @tag :skip
    test "with schema" do
      accounts =
        Stream.map(1..10000, fn i ->
          %{id: i, name: "John-#{i}", inserted_at: naive_now(), updated_at: naive_now()}
        end)

      assert {10000, nil} =
               TestRepo.checkout(fn ->
                 Enum.into(accounts, TestRepo.stream(Account))
               end)
    end

    @tag :skip
    test "with table" do
      accounts =
        Stream.map(1..10000, fn i ->
          [id: i, name: "John-#{i}", inserted_at: naive_now(), updated_at: naive_now()]
        end)

      assert {10000, nil} =
               TestRepo.checkout(fn ->
                 Enum.into(
                   accounts,
                   TestRepo.stream(
                     "accounts",
                     types: [
                       id: :u64,
                       name: :string,
                       inserted_at: :datetime,
                       updated_at: :datetime
                     ]
                   )
                 )
               end)
    end
  end

  defp naive_now do
    NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)
  end
end
