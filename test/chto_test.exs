defmodule ChtoTest do
  use ExUnit.Case
  import Ecto.Query
  alias Ecto.Adapters.ClickHouse.Connection, as: SQL

  defp to_sql(query, opts \\ []) do
    {query, _params, _key} = Ecto.Query.Planner.plan(query, :all, Ecto.Adapters.ClickHouse)
    {query, _} = Ecto.Query.Planner.normalize(query, :all, Ecto.Adapters.ClickHouse, _counter = 0)

    query
    |> tap(fn query ->
      if opts = opts[:inspect] do
        IO.inspect(Map.from_struct(query), opts)
      end
    end)
    |> SQL.all()
    |> IO.iodata_to_binary()
  end

  describe "SQL" do
    test "all" do
      query = select("events", [e], e.name)
      # TODO drop AS?
      assert to_sql(query) == ~s[SELECT e0."name" FROM "events" AS e0]

      query = select("events", [e], map(e, [:name, :user_id]))
      assert to_sql(query) == ~s[SELECT e0."name",e0."user_id" FROM "events" AS e0]

      query = "events" |> select([e], e.name) |> limit(1)
      assert to_sql(query) == ~s[SELECT e0."name" FROM "events" AS e0 LIMIT 1]

      name = "John"
      query = "events" |> where(name: type(^name, :string)) |> select([e], e.user_id)

      assert to_sql(query) ==
               ~s[SELECT e0."user_id" FROM "events" AS e0 WHERE (e0."name" = {var:String})]

      query =
        "events"
        |> where([e], fragment("name = ?", type(^name, :string)))
        |> select([e], e.user_id)

      assert to_sql(query) ==
               ~s[SELECT e0."user_id" FROM "events" AS e0 WHERE (name = {var:String})]
    end
  end
end
