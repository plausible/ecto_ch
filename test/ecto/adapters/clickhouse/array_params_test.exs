defmodule Ecto.Adapters.ClickHouse.ArrayParamsTest do
  use ExUnit.Case, async: true

  alias Ecto.Adapters.ClickHouse.Connection

  test "integer arrays infer a type containing every value" do
    for {values, type} <- [
          {[1, Integer.pow(2, 63)], "UInt64"},
          {[1, Integer.pow(2, 64)], "UInt128"},
          {[1, Integer.pow(2, 128)], "UInt256"},
          {[-1, Integer.pow(2, 63)], "Int128"},
          {[-1, Integer.pow(2, 127)], "Int256"},
          {[-Integer.pow(2, 255), 1], "Int256"}
        ],
        values <- [values, Enum.reverse(values)] do
      assert param(values) == "{$0:Array(#{type})}"
    end
  end

  test "integer arrays reject values without a lossless common integer type" do
    for values <- [
          [-1, Integer.pow(2, 255)],
          [0, Integer.pow(2, 256)],
          [0, -Integer.pow(2, 255) - 1]
        ],
        values <- [values, Enum.reverse(values)] do
      assert_raise ArgumentError, ~r/common ClickHouse integer type.*explicit type\/2/, fn ->
        param(values)
      end
    end
  end

  test "decimal arrays combine integer digits and scale" do
    for values <- permutations([Decimal.new("123.4"), Decimal.new("0.123"), 2]) do
      assert param(values) == "{$0:Array(Decimal(6,3))}"
    end

    assert param([Decimal.new("1E-76"), 0]) == "{$0:Array(Decimal(76,76))}"
    assert param([Decimal.new("0"), Decimal.new("1E-76")]) == "{$0:Array(Decimal(76,76))}"

    for values <- permutations([Decimal.new("1E75"), Decimal.new("1E-75"), 0]) do
      assert_raise ArgumentError, ~r/decimal precision exceeds maximum 76/, fn ->
        param(values)
      end
    end

    assert_raise ArgumentError, ~r/Decimal values must be finite/, fn ->
      param([Decimal.new(1), Decimal.new("NaN")])
    end
  end

  test "temporal arrays use the greatest precision and range" do
    for values <-
          permutations([
            ~N[2020-01-01 00:00:00],
            ~N[2020-01-01 00:00:00.123],
            ~N[2020-01-01 00:00:00.123456]
          ]) do
      assert param(values) == "{$0:Array(DateTime64(6))}"
    end

    for values <- permutations([~T[12:00:00], ~T[12:00:00.123], ~T[12:00:00.123456]]) do
      assert param(values) == "{$0:Array(Time64(6))}"
    end

    assert param([~U[2020-01-01 00:00:00Z], ~U[2200-01-01 00:00:00Z]]) ==
             "{$0:Array(DateTime64(0))}"

    assert param([~N[2020-01-01 00:00:00], ~N[2106-02-07 06:28:16]]) ==
             "{$0:Array(DateTime64(0))}"

    assert param([~N[2020-01-01 00:00:00], ~N[1969-12-31 23:59:59]]) ==
             "{$0:Array(DateTime64(0))}"

    assert param([~D[2020-01-01], ~D[2200-01-01]]) == "{$0:Array(Date32)}"
  end

  test "empty nested arrays retain their dimensions" do
    assert param([]) == "{$0:Array(Nothing)}"
    assert param([[], []]) == "{$0:Array(Array(Nothing))}"
    assert param([[], [[]]]) == "{$0:Array(Array(Array(Nothing)))}"

    for values <- permutations([[], [[]], [[1, Integer.pow(2, 64)]]]) do
      assert param(values) == "{$0:Array(Array(Array(UInt128)))}"
    end
  end

  test "nullable types are inferred at each scalar depth" do
    for values <- permutations([nil, 1, Integer.pow(2, 64)]) do
      assert param(values) == "{$0:Array(Nullable(UInt128))}"
    end

    assert param([nil, nil]) == "{$0:Array(Nullable(Nothing))}"
    assert param([[], [nil, 1]]) == "{$0:Array(Array(Nullable(Int64)))}"
  end

  test "arrays of maps inspect every key and value" do
    values = [%{"a" => 1}, %{"b" => Integer.pow(2, 64), "c" => nil}]
    assert param(values) == "{$0:Array(Map(String,Nullable(UInt128)))}"
    assert param(Enum.reverse(values)) == param(values)
  end

  test "ambiguous mixtures and invalid nullable containers require an explicit type" do
    for values <- [
          [1, 0.5],
          [1, "1"],
          [true, 1],
          [Decimal.new(1), 0.5],
          [~D[2020-01-01], ~N[2020-01-01 00:00:00]],
          [[], nil],
          [%{}, nil],
          [%{nil => 1}]
        ],
        values <- [values, Enum.reverse(values)] do
      assert_raise ArgumentError,
                   ~r/cannot infer an array parameter type:.*explicit type\/2/,
                   fn ->
                     param(values)
                   end
    end
  end

  test "temporal arrays reject values outside supported ranges" do
    for values <- [[~D[2020-01-01], ~D[2300-01-01]], [~N[1899-01-01 00:00:00]]] do
      assert_raise ArgumentError, ~r/outside the .* range/, fn -> param(values) end
    end
  end

  defp param(value), do: to_string(Connection.build_params(0, 1, [value]))

  defp permutations([]), do: [[]]

  defp permutations(values) do
    for value <- values, rest <- permutations(List.delete(values, value)), do: [value | rest]
  end
end
