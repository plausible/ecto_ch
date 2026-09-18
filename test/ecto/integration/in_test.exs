defmodule Ecto.Integration.InTest do
  use Ecto.Integration.Case, async: false

  import Ecto.Query
  alias Ecto.Integration.TestRepo
  alias EctoClickHouse.Integration.Product

  # `x in ^list` is sent as a single Array(...) param, so these check that the
  # inferred element type actually round-trips every value in the list rather
  # than letting ClickHouse coerce the ones that don't fit the first element.

  setup do
    TestRepo.insert_all(Product, [
      %{id: 1, name: "hello", price: Decimal.new("1.00")},
      %{id: 2, name: "", price: Decimal.new("2.12")},
      %{id: 3, name: "world", price: Decimal.new("3.00")}
    ])

    :ok
  end

  defp names(list) do
    TestRepo.all(from p in Product, where: p.name in ^list, select: p.name, order_by: p.name)
  end

  defp ids(list) do
    TestRepo.all(from p in Product, where: p.id in ^list, select: p.id, order_by: p.id)
  end

  test "matches the rows the list names" do
    assert names(["hello", "world"]) == ["hello", "world"]
    assert ids([1, 3]) == [1, 3]
  end

  test "nil does not match the element type's default value" do
    # Array(String) would store nil as "" and wrongly match the empty-name row
    assert names(["hello", nil]) == ["hello"]
    assert names([nil, "hello"]) == ["hello"]
    assert names([nil]) == []
  end

  test "integers wider than the first element do not wrap around" do
    # 99_999_999_999_999_999_999 wraps to 7_766_279_631_452_241_919 inside
    # Array(Int64), which would make this row a false positive
    wraps_to = 7_766_279_631_452_241_919
    too_wide = 99_999_999_999_999_999_999

    TestRepo.insert_all(Product, [
      %{id: wraps_to, name: "wrapped", price: Decimal.new("1.00")}
    ])

    assert ids([1, too_wide]) == [1]
    assert ids([too_wide, 1]) == [1]
    assert ids([1, wraps_to]) == [1, wraps_to]
  end

  test "decimals more precise than the first element are not rounded" do
    query = fn list ->
      TestRepo.all(from p in Product, where: p.price in ^list, select: p.id, order_by: p.id)
    end

    # inside Array(Decimal(2,1)) the second value rounds to 2.1 and matches nothing
    assert query.([Decimal.new("1.0"), Decimal.new("2.12")]) == [1, 2]
    assert query.([Decimal.new("2.12"), Decimal.new("1.0")]) == [1, 2]
  end

  test "sub-second datetimes are not truncated to the first element's precision" do
    TestRepo.insert_all(Product, [
      %{id: 4, name: "approved", price: Decimal.new("1.00"), approved_at: ~N[2020-01-01 00:00:00]}
    ])

    # a whole-second datetime first, a sub-second one second: ClickHouse cannot
    # parse a fractional timestamp as Array(DateTime) at all, so this would fail
    # outright if the precision came from the head of the list
    approved_at = [~N[2019-01-01 00:00:00], ~N[2020-01-01 00:00:00.000000]]

    query =
      from p in Product,
        where: fragment("toDateTime64(?,6)", p.approved_at) in ^approved_at,
        select: p.id

    assert TestRepo.all(query) == [4]
  end

  test "lists with no common type raise instead of returning coerced rows" do
    assert_raise ArgumentError, ~r/have no common type/, fn ->
      TestRepo.all(from p in Product, where: fragment("?", p.id) in ^[1, "a"], select: p.id)
    end
  end
end
