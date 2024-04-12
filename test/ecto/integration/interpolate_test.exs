defmodule Ecto.Integration.InterpolateTest do
  use Ecto.Integration.Case, async: true
  import Ecto.Query
  alias Ecto.Integration.TestRepo

  def all(query) do
    {sql, _params} = TestRepo.to_sql(:all, query, interpolate: true)
    %{sql: sql, rows: TestRepo.query!(sql, _no_params = []).rows}
  end

  describe "to_sql and exec" do
    test "with integers" do
      assert all(
               from n in fragment("numbers(3)"),
                 where: n.number > ^1 and ^1 > 0,
                 having: 5 > ^4,
                 limit: ^200,
                 select: n.number
             ) == %{
               sql:
                 ~s{SELECT f0."number" FROM numbers(3) AS f0 WHERE ((f0."number" > 1) AND (1 > 0)) HAVING (5 > 4) LIMIT 200},
               rows: [[2]]
             }
    end
  end
end
