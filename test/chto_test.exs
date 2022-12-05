defmodule ChtoTest do
  use ExUnit.Case
  import Ecto.Query
  alias Ecto.Adapters.ClickHouse.Connection, as: SQL

  defp to_sql(query, opts \\ []) do
    # TODO
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
      # Rexbug.start("Ecto.Adapters.ClickHouse.Connection :: return", msgs: 10000)

      query = select(Event, [e], e.name)
      # TODO drop AS?
      assert to_sql(query) == ~s[SELECT e0."name" FROM "events" AS e0]

      query = select(Event, [e], map(e, [:name, :user_id]))
      assert to_sql(query) == ~s|SELECT e0."name",e0."user_id" FROM "events" AS e0|

      query = Event |> select([e], e.name) |> limit(1)
      assert to_sql(query) == ~s|SELECT e0."name" FROM "events" AS e0 LIMIT 1|

      # params
      name = "John"
      query = Event |> where(name: type(^name, :string)) |> select([e], e.user_id)

      assert to_sql(query) ==
               ~s|SELECT e0."user_id" FROM "events" AS e0 WHERE (e0."name" = {$0:String})|

      # fragments
      name = "John"

      query =
        Event
        |> where([e], fragment("name = ?", type(^name, :string)))
        |> select([e], e.user_id)

      assert to_sql(query) ==
               ~s|SELECT e0."user_id" FROM "events" AS e0 WHERE (name = {$0:String})|

      # inferred type
      query =
        Event
        |> where(name: ^name)
        |> select([e], e.user_id)

      assert to_sql(query) ==
               ~s|SELECT e0."user_id" FROM "events" AS e0 WHERE (e0."name" = {$0:String})|

      # in
      names = ["John", "Jack"]
      query = Event |> where([e], e.name in ^names) |> select([e], e.user_id)

      # TODO or SELECT e0."user_id" FROM "events" AS e0 WHERE (e0."name" in ARRAY[{$0:String},{$1:String}])
      assert to_sql(query) ==
               ~s|SELECT e0."user_id" FROM "events" AS e0 WHERE (e0."name" IN {$0:Array(String)})|

      # subquery
      name = "Jack"

      query =
        from e in Event,
          order_by: [desc: :user_id],
          limit: 10,
          where: e.name == ^name,
          select: e.user_id

      query = from e in subquery(query), select: avg(e.user_id), where: e.user_id > 10
      assert to_sql(query, inspect: []) == ""
    end

    test "new" do
      Rexbug.start(["Ecto.Query", "Ecto.Query.Builder", "Ecto.Query.Builder.Filter"], msgs: 10000)
      name = "Jack"

      query =
        from e in Event,
          order_by: [desc: :user_id],
          limit: 10,
          where: e.name == ^name,
          select: e.user_id

      query = from e in subquery(query), select: avg(e.user_id), where: e.user_id > 10
      :timer.sleep(100)
      IO.puts("done")
      assert to_sql(query) == ""
    end

    # TODO
    # test "ix?"
  end
end
