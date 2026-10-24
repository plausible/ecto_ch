defmodule Ecto.Adapters.ClickHouse.InputTest do
  use Ecto.Integration.Case

  import Ecto.Query
  import Ecto.Adapters.ClickHouse.API, only: [input: 1]

  alias Ecto.Integration.TestRepo

  setup do
    TestRepo.query!("""
    CREATE TABLE input_rows (
      id UInt64,
      name String DEFAULT 'unselected'
    ) ENGINE Memory
    """)

    on_exit(fn -> TestRepo.query!("DROP TABLE input_rows") end)
    :ok
  end

  test "input fields can be projected into differently named destination columns" do
    query =
      from i in input(raw_id: "UInt64", raw_name: "String"),
        select: %{id: i.raw_id, name: fragment("upperUTF8(?)", i.raw_name)}

    rows = [%{raw_id: 1, raw_name: "first"}, %{raw_id: 2, raw_name: "second"}]

    assert {2, nil} = TestRepo.insert_all("input_rows", rows, input: query)
    assert stored_rows() == [[1, "FIRST"], [2, "SECOND"]]
  end

  test "input declaration order is independent of row and destination field order" do
    query =
      from i in input(raw_name: "String", raw_id: "UInt64"),
        select: %{id: i.raw_id, name: i.raw_name}

    rows = [[raw_id: 1, raw_name: "first"], %{raw_name: "second", raw_id: 2}]

    assert {2, nil} = TestRepo.insert_all("input_rows", rows, input: query)
    assert stored_rows() == [[1, "first"], [2, "second"]]
  end

  test "input queries can select a subset of the input fields" do
    query =
      from i in input(id: "UInt64", ignored: "String"),
        select: %{id: i.id}

    rows = [%{id: 1, ignored: "first"}, %{id: 2, ignored: "second"}]

    assert {2, nil} = TestRepo.insert_all("input_rows", rows, input: query)
    assert stored_rows() == [[1, "unselected"], [2, "unselected"]]
  end

  test "input queries accept map selections" do
    query =
      from i in input(id: "UInt64", name: "String"),
        select: map(i, [:name, :id])

    assert {2, nil} =
             TestRepo.insert_all(
               "input_rows",
               [%{id: 1, name: "first"}, %{id: 2, name: "second"}],
               input: query
             )

    assert stored_rows() == [[1, "first"], [2, "second"]]
  end

  test "input queries reject ordinary table sources without inserting rows" do
    query = from i in "input_rows", select: %{id: i.id, name: i.name}

    assert_raise ArgumentError, ~r/unexpected source found in input query/, fn ->
      TestRepo.insert_all("input_rows", [%{id: 1, name: "first"}], input: query)
    end

    assert stored_rows() == []
  end

  test "input queries reject bound parameters without inserting rows" do
    query =
      from i in input(id: "UInt64", name: "String"),
        select: %{id: i.id, name: ^"override"}

    assert_raise ArgumentError, ~r/cannot insert from parameterized input query/, fn ->
      TestRepo.insert_all("input_rows", [%{id: 1, name: "first"}], input: query)
    end

    assert stored_rows() == []
  end

  test "input queries reject scalar selections without inserting rows" do
    query = from i in input(id: "UInt64"), select: i.id

    assert_raise ArgumentError, ~r/cannot generate a fields list/, fn ->
      TestRepo.insert_all("input_rows", [%{id: 1}], input: query)
    end

    assert stored_rows() == []
  end

  defp stored_rows do
    TestRepo.query!("SELECT id, name FROM input_rows ORDER BY id").rows
  end
end
