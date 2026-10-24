defmodule Ecto.Integration.ArrayParamsTest do
  use Ecto.Integration.Case, async: true

  import Ecto.Query
  alias Ecto.Integration.TestRepo

  defmodule TypedArrays do
    use Ecto.Schema

    @primary_key false
    @schema_prefix "system"
    schema "one" do
      field(:values, Ch, type: "Array(UInt128)")
      field(:nested_values, {:array, Ch}, type: "Array(UInt128)")
    end
  end

  test "array parameters preserve every integer regardless of order" do
    for values <- [
          [1, Integer.pow(2, 64)],
          [-1, Integer.pow(2, 64)],
          [-1, Integer.pow(2, 127)],
          [1, Integer.pow(2, 128)],
          [-1, -Integer.pow(2, 255)],
          [0, Integer.pow(2, 256) - 1]
        ],
        values <- [values, Enum.reverse(values)] do
      assert round_trip(values) == values
    end
  end

  test "array parameters combine decimal integer digits and fractional precision" do
    values = [Decimal.new("1.2"), Decimal.new("123.456")]
    expected = [Decimal.new("1.200"), Decimal.new("123.456")]

    assert round_trip(values) == expected
    assert round_trip(Enum.reverse(values)) == Enum.reverse(expected)
  end

  test "array parameters combine integers and decimals without rounding" do
    values = [10_000, Decimal.new("0.01")]
    expected = [Decimal.new("10000.00"), Decimal.new("0.01")]

    assert round_trip(values) == expected
    assert round_trip(Enum.reverse(values)) == Enum.reverse(expected)
  end

  test "array parameters preserve dates outside the Date range" do
    values = [~D[2020-01-01], ~D[2200-01-01]]

    assert round_trip(values) == values
    assert round_trip(Enum.reverse(values)) == Enum.reverse(values)
  end

  test "array parameters preserve the greatest datetime precision" do
    values = [~N[2020-01-01 12:34:56], ~N[2020-01-01 12:34:56.123456]]

    expected =
      Enum.map(
        [~N[2020-01-01 12:34:56.000000], ~N[2020-01-01 12:34:56.123456]],
        &to_clickhouse_naive/1
      )

    assert round_trip(values) == expected
    assert round_trip(Enum.reverse(values)) == Enum.reverse(expected)
  end

  test "array parameters preserve whole-second datetimes outside the DateTime range" do
    values = [~N[2020-01-01 12:34:56], ~N[2200-01-01 12:34:56]]
    expected = Enum.map(values, &to_clickhouse_naive/1)

    assert round_trip(values) == expected
    assert round_trip(Enum.reverse(values)) == Enum.reverse(expected)
  end

  test "array parameters preserve aware datetimes and their greatest precision" do
    values = [
      DateTime.shift_zone!(~U[2020-01-01 12:34:56Z], "Asia/Bangkok"),
      ~U[2020-01-01 12:34:56.123456Z]
    ]

    expected = [~N[2020-01-01 12:34:56.000000], ~N[2020-01-01 12:34:56.123456]]

    assert round_trip(values) == expected
    assert round_trip(Enum.reverse(values)) == Enum.reverse(expected)
  end

  test "arrays of maps infer every value" do
    values = [%{"small" => 1}, %{"large" => Integer.pow(2, 64), "absent" => nil}]

    assert round_trip(values) == values
    assert round_trip(Enum.reverse(values)) == Enum.reverse(values)
  end

  test "array parameters preserve nullable values and widen non-null integers" do
    values = [nil, 1, Integer.pow(2, 64), nil]

    assert round_trip(values) == values
    assert round_trip(Enum.reverse(values)) == Enum.reverse(values)
    assert round_trip([nil, nil]) == [nil, nil]
  end

  test "nested array parameters infer values after empty arrays and nulls" do
    values = [[], [nil, 1], [], [Integer.pow(2, 64), nil]]

    assert round_trip(values) == values
    assert round_trip(Enum.reverse(values)) == Enum.reverse(values)

    values = [[], [[]], [[1, Integer.pow(2, 64)]]]
    assert round_trip(values) == values
  end

  test "explicit array types apply before parameter parsing" do
    values = [1, Integer.pow(2, 64)]
    array_type = Ecto.ParameterizedType.init(Ch, type: "Array(UInt128)")

    query = from fragment("system.one"), select: type(^values, ^array_type)

    {sql, [^values]} = TestRepo.to_sql(:all, query)
    assert sql =~ "{$0:Array(UInt128)}"
    assert TestRepo.all(query) == [values]
  end

  test "explicit schema array types apply to direct parameters" do
    values = [1, Integer.pow(2, 64)]
    nested_values = [[], values]

    query =
      from row in TypedArrays,
        select: {type(^values, row.values), type(^nested_values, row.nested_values)}

    {sql, [^values, ^nested_values]} = TestRepo.to_sql(:all, query)
    assert sql =~ "{$0:Array(UInt128)}"
    assert sql =~ "{$1:Array(Array(UInt128))}"
    assert TestRepo.all(query) == [{values, nested_values}]
  end

  test "explicit array types support intentionally mixed values" do
    float_type = Ecto.ParameterizedType.init(Ch, type: "Array(Float64)")
    enum_type = Ecto.ParameterizedType.init(Ch, type: "Array(Enum8('foo'=1,'bar'=2))")

    query =
      from fragment("system.one"),
        select: {type(^[1, 2.5], ^float_type), type(^["foo", "bar"], ^enum_type)}

    assert TestRepo.all(query) == [{[1.0, 2.5], ["foo", "bar"]}]
  end

  test "explicit array types preserve UUIDs and nullable values" do
    uuid = "601d74e4-a8d3-4b6e-8365-eddb4c893327"
    uuid_type = Ecto.ParameterizedType.init(Ch, type: "Array(Nullable(UUID))")

    query = from fragment("system.one"), select: type(^[nil, uuid], ^uuid_type)

    assert TestRepo.all(query) == [[nil, uuid]]
  end

  test "ambiguous array values ask for an explicit type" do
    for values <- [
          [1, 2.5],
          [9_007_199_254_740_993, 0.5],
          [~D[2020-01-01], ~N[2020-01-01 00:00:00]],
          ["foo", 2]
        ],
        values <- [values, Enum.reverse(values)] do
      assert_raise ArgumentError, ~r/explicit.*type/i, fn -> round_trip(values) end
    end
  end

  defp round_trip(values) do
    query = from fragment("system.one"), select: fragment("?", ^values)
    assert [result] = TestRepo.all(query)
    result
  end
end
