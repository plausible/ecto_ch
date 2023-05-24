defmodule Ecto.Integration.SchemalessTest do
  use Ecto.Integration.Case
  alias Ecto.Integration.TestRepo

  describe "insert_stream/3" do
    setup do
      accounts =
        Stream.map(1..10000, fn i ->
          %{id: i, name: "John-#{i}", inserted_at: naive_now(), updated_at: naive_now()}
        end)

      {:ok, accounts: accounts}
    end

    test "with atom types", %{accounts: accounts} do
      types = [id: :u64, name: :string, inserted_at: :datetime, updated_at: :datetime]
      assert {10000, nil} = TestRepo.insert_stream("accounts", accounts, types: types)
    end

    test "with type helpers", %{accounts: accounts} do
      types = [
        id: Ch.Types.u64(),
        name: Ch.Types.string(),
        inserted_at: Ch.Types.datetime(),
        updated_at: Ch.Types.datetime()
      ]

      assert {10000, nil} = TestRepo.insert_stream("accounts", accounts, types: types)
    end

    test "with string types", %{accounts: accounts} do
      types = [id: "UInt64", name: "String", inserted_at: "DateTime", updated_at: "DateTime"]
      assert {10000, nil} = TestRepo.insert_stream("accounts", accounts, types: types)
    end
  end

  describe "insert_all/3" do
    setup do
      accounts =
        Enum.map(1..1000, fn i ->
          [id: i, name: "John-#{i}", inserted_at: naive_now(), updated_at: naive_now()]
        end)

      {:ok, accounts: accounts}
    end

    test "with atom types", %{accounts: accounts} do
      types = [id: :u64, name: :string, inserted_at: :datetime, updated_at: :datetime]
      assert {1000, nil} = TestRepo.insert_all("accounts", accounts, types: types)
    end

    test "with type helpers", %{accounts: accounts} do
      types = [
        id: Ch.Types.u64(),
        name: Ch.Types.string(),
        inserted_at: Ch.Types.datetime(),
        updated_at: Ch.Types.datetime()
      ]

      assert {1000, nil} = TestRepo.insert_all("accounts", accounts, types: types)
    end

    test "with string types", %{accounts: accounts} do
      types = [id: "UInt64", name: "String", inserted_at: "DateTime", updated_at: "DateTime"]
      assert {1000, nil} = TestRepo.insert_all("accounts", accounts, types: types)
    end
  end

  defp naive_now do
    NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)
  end
end
