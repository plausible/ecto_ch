defmodule Ecto.Integration.InterpolateTest do
  use Ecto.Integration.Case, async: true
  import Ecto.Query
  alias Ecto.Integration.TestRepo

  def all(query) do
    {sql, _params} = TestRepo.to_sql(:all, query, interpolate: true)
    %{sql: sql, rows: TestRepo.query!(sql, _no_params = []).rows}
  end

  describe "interpolate and exec" do
    # https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
    test "with integers" do
      assert all(
               from n in fragment("numbers(3)"),
                 where: n.number > ^1 and ^18_446_744_073_709_551_615 > 0,
                 having: 5 > ^(-9_223_372_036_854_775_808),
                 limit: ^2_147_483_647,
                 select: n.number
             ) == %{
               sql:
                 ~s{SELECT f0."number" FROM numbers(3) AS f0 WHERE ((f0."number" > 1) AND (18446744073709551615 > 0)) HAVING (5 > -9223372036854775808) LIMIT 2147483647},
               rows: [[2]]
             }
    end

    # https://clickhouse.com/docs/en/sql-reference/data-types/float
    test "with floats"
  end
end
