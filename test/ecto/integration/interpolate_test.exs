defmodule Ecto.Integration.InterpolateTest do
  use Ecto.Integration.Case
  import Ecto.Query
  alias Ecto.Integration.TestRepo

  def all(query) do
    {sql, _params} = TestRepo.to_sql(:all, query, interpolate: true)
    %{sql: sql, rows: TestRepo.query!(sql, _no_params = []).rows}
  end

  describe "interpolate and exec" do
    # https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
    test "with integers" do
      uint64_max = 18_446_744_073_709_551_615
      int64_min = -9_223_372_036_854_775_808
      int32_max = 2_147_483_647

      assert all(
               from n in fragment("numbers(3)"),
                 where: n.number > ^1 and ^uint64_max > 0,
                 having: 5 > ^int64_min,
                 limit: ^int32_max,
                 select: n.number
             ) == %{
               rows: [[2]],
               sql:
                 ~s{SELECT f0."number" FROM numbers(3) AS f0 WHERE ((f0."number" > 1) AND (18446744073709551615 > 0)) HAVING (5 > -9223372036854775808) LIMIT 2147483647}
             }
    end

    # https://clickhouse.com/docs/en/sql-reference/data-types/float
    test "with floats" do
      almost_zero = 0.09999999999999998
      will_be_rounded_to_one = 0.9999999999999999999999999
      will_be_scientific = 10_000_000_000_000_000_000_000_000_000_000_000_000.0

      assert all(
               from n in fragment("numbers(3)"),
                 where: n.number > ^almost_zero and 0.0 < ^will_be_rounded_to_one,
                 having: 5.0 < ^will_be_scientific,
                 select: n.number
             ) == %{
               rows: [[1], [2]],
               sql:
                 ~s{SELECT f0."number" FROM numbers(3) AS f0 WHERE ((f0."number" > 0.09999999999999998) AND (0.0 < 1.0)) HAVING (5.0 < 1.0e37)}
             }
    end

    # https://clickhouse.com/docs/en/sql-reference/data-types/decimal
    test "with decimals" do
      # no rounding for decimals!
      almost_zero = Decimal.new("0.09999999999999998")
      alomost_one = Decimal.new("0.9999999999999999999999999")
      big = Decimal.new(10_000_000_000_000_000_000_000_000_000_000_000_000)

      assert all(
               from n in fragment("numbers(3)"),
                 where: n.number > ^almost_zero and n.number > ^alomost_one,
                 having: n.number < ^big,
                 select: n.number
             ) == %{
               rows: [[2]],
               sql: """
               SELECT f0."number" FROM numbers(3) AS f0 \
               WHERE ((f0."number" > 0.09999999999999998) AND (f0."number" > 0.9999999999999999999999999)) \
               HAVING (f0."number" < 10000000000000000000000000000000000000)\
               """
             }
    end

    # https://clickhouse.com/docs/en/sql-reference/data-types/string
    test "with strings" do
      trouble1 = "'\\  "
      trouble2 = "'"

      assert all(
               from n in fragment("numbers(3)"),
                 where: fragment("toString(?)", n.number) == ^"2" and ^"asdfgh" != "qwerty",
                 having:
                   ^trouble1 == selected_as(:trouble1) and ^trouble2 == selected_as(:trouble2),
                 select: [n.number, selected_as("'\\  ", :trouble1), selected_as("'", :trouble2)]
             ) == %{
               rows: [[2, trouble1, trouble2]],
               sql: """
               SELECT f0."number",'''\\\\  ' AS "trouble1",'''' AS "trouble2" \
               FROM numbers(3) AS f0 \
               WHERE ((toString(f0."number") = '2') AND ('asdfgh' != 'qwerty')) \
               HAVING (('''\\\\  ' = "trouble1") AND ('''' = "trouble2"))\
               """
             }
    end

    # https://clickhouse.com/docs/en/sql-reference/data-types/date
    test "with dates" do
      flight_to_cnx = ~D[2024-04-30]

      assert all(
               from n in fragment("numbers(3)"),
                 where: selected_as(:date) == ^flight_to_cnx,
                 select: [n.number, selected_as(fragment("toDate(?+19842)", n.number), :date)]
             ) == %{
               rows: [[1, flight_to_cnx]],
               sql: """
               SELECT f0."number",toDate(f0."number"+19842) AS "date" \
               FROM numbers(3) AS f0 \
               WHERE ("date" = '2024-04-30')\
               """
             }
    end
  end
end
