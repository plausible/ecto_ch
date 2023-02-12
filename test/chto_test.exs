defmodule ChtoTest do
  use ExUnit.Case
  import Ecto.Query
  alias Ecto.Adapters.ClickHouse.Connection, as: SQL

  describe "all" do
    test "select one column" do
      query = select("events", [e], e.name)
      # TODO drop AS?
      assert all(query) == {~s[SELECT e0."name" FROM "events" AS e0], []}
    end

    test "select two columns" do
      query = select("events", [e], map(e, [:name, :user_id]))
      assert all(query) == {~s[SELECT e0."name",e0."user_id" FROM "events" AS e0], []}
    end

    test "limit" do
      query = "events" |> select([e], e.name) |> limit(1)
      assert all(query) == {~s[SELECT e0."name" FROM "events" AS e0 LIMIT 1], []}
    end

    test "where with typed param" do
      name = "John"
      min_user_id = 10

      query =
        "events"
        |> where(name: ^name)
        |> where([e], e.user_id > ^min_user_id)
        |> select([e], e.user_id)

      assert all(query) ==
               {~s[SELECT e0."user_id" FROM "events" AS e0 WHERE (e0."name" = {$0:String}) AND (e0."user_id" > {$1:Int64})],
                ["John", 10]}
    end

    test "where with fragment" do
      name = "John"

      query =
        "events"
        |> where([e], fragment("name = ?", ^name))
        |> select([e], e.user_id)

      assert all(query) ==
               {~s[SELECT e0."user_id" FROM "events" AS e0 WHERE (name = {$0:String})], ["John"]}
    end

    test "where in" do
      domains = ["dummy.site", "dummy2.site"]
      date_range = %{first: ~D[2020-10-10], last: ~D[2021-01-01]}

      query =
        from e in "events",
          where: e.domain in ^domains,
          where: fragment("toDate(?)", e.timestamp) >= ^date_range.first,
          where: fragment("toDate(?)", e.timestamp) <= ^date_range.last,
          select: {
            fragment("countIf(? = 'pageview')", e.name),
            fragment("countIf(? != 'pageview')", e.name)
          }

      assert all(query) ==
               {"SELECT countIf(e0.\"name\" = 'pageview'),countIf(e0.\"name\" != 'pageview') FROM \"events\" AS e0 WHERE (e0.\"domain\" IN {$0:Array(String)}) AND (toDate(e0.\"timestamp\") >= {$1:Date}) AND (toDate(e0.\"timestamp\") <= {$2:Date})",
                [["dummy.site", "dummy2.site"], ~D[2020-10-10], ~D[2021-01-01]]}
    end
  end

  defp all(query) do
    to_sql(query, fn query, params -> SQL.all(query, params) end)
  end

  defp to_sql(query, f) do
    {query, params, _key} = Ecto.Query.Planner.plan(query, :all, Ecto.Adapters.ClickHouse)
    {query, _} = Ecto.Query.Planner.normalize(query, :all, Ecto.Adapters.ClickHouse, _counter = 0)
    {dump_params, _} = Enum.unzip(params)
    {IO.iodata_to_binary(f.(query, dump_params)), dump_params}
  end

  def i(query) do
    IO.inspect(Map.from_struct(query), limit: :infinity)
  end

  @tag db: true
  test "to_sql" do
    start_supervised!(Repo)

    user_id = 1
    query = "example" |> where([e], e.user_id == ^user_id) |> select([e], e.name)
    assert {sql, params} = Repo.to_sql(:all, query)
    assert sql == ~s[SELECT e0."name" FROM "example" AS e0 WHERE (e0."user_id" = {$0:Int64})]
    assert params == [1]
  end
end
