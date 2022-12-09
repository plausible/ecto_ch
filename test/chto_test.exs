defmodule ChtoTest do
  use ExUnit.Case
  import Ecto.Query
  alias Ecto.Adapters.ClickHouse.Connection, as: SQL

  describe "all" do
    test "select one column" do
      query = select("events", [e], e.name)
      # TODO drop AS?
      assert all(query) == ~s[SELECT e0."name" FROM "events" AS e0]
    end

    test "select two columns" do
      query = select("events", [e], map(e, [:name, :user_id]))
      assert all(query) == ~s[SELECT e0."name",e0."user_id" FROM "events" AS e0]
    end

    test "limit" do
      query = "events" |> select([e], e.name) |> limit(1)
      assert all(query) == ~s[SELECT e0."name" FROM "events" AS e0 LIMIT 1]
    end

    test "where with typed param" do
      name = "John"
      min_user_id = 10

      query =
        "events"
        |> where(name: type(^name, :string))
        |> where([e], e.user_id > type(^min_user_id, :integer))
        |> select([e], e.user_id)

      assert all(query) ==
               ~s[SELECT e0."user_id" FROM "events" AS e0 WHERE (e0."name" = {$0:String}) AND (e0."user_id" > {$1:Int64})]
    end

    test "where with fragment" do
      name = "John"

      query =
        "events"
        |> where([e], fragment("name = ?", type(^name, :string)))
        |> select([e], e.user_id)

      assert all(query) ==
               ~s[SELECT e0."user_id" FROM "events" AS e0 WHERE (name = {$0:String})]
    end
  end

  defp all(query) do
    to_sql(query, fn query -> SQL.all(query) end)
  end

  defp to_sql(query, f) do
    {query, _params, _key} = Ecto.Query.Planner.plan(query, :all, Ecto.Adapters.ClickHouse)
    {query, _} = Ecto.Query.Planner.normalize(query, :all, Ecto.Adapters.ClickHouse, _counter = 0)
    query |> f.() |> IO.iodata_to_binary()
  end

  def i(query) do
    IO.inspect(Map.from_struct(query), limit: :infinity)
  end
end
