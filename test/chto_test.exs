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

      # %{
      #   aliases: %{},
      #   assocs: [],
      #   combinations: [],
      #   distinct: nil,
      #   from: %Ecto.Query.FromExpr{
      #     source: {"events", Event},
      #     file: nil,
      #     line: nil,
      #     as: nil,
      #     prefix: nil,
      #     params: [],
      #     hints: []
      #   },
      #   group_bys: [],
      #   havings: [],
      #   joins: [],
      #   limit: nil,
      #   lock: nil,
      #   offset: nil,
      #   order_bys: [],
      #   prefix: nil,
      #   preloads: [],
      #   select: %Ecto.Query.SelectExpr{
      #     expr: {{:., [type: :string], [{:&, [], [0]}, :name]}, [], []},
      #     file: "/Users/q/Developer/chto/test/chto_test.exs",
      #     line: 64,
      #     fields: [{{:., [type: :string], [{:&, [], [0]}, :name]}, [], []}],
      #     params: nil,
      #     take: %{},
      #     subqueries: [],
      #     aliases: %{}
      #   },
      #   sources: {{"events", Event, nil}},
      #   updates: [],
      #   wheres: [],
      #   windows: [],
      #   with_ctes: nil
      # }
      query = select(Event, [e], e.name)
      # TODO drop AS?
      assert to_sql(query) == ~s[SELECT e0."name" FROM "events" AS e0]

      # %{
      #   aliases: %{},
      #   assocs: [],
      #   combinations: [],
      #   distinct: nil,
      #   from: %Ecto.Query.FromExpr{
      #     source: {"events", Event},
      #     file: nil,
      #     line: nil,
      #     as: nil,
      #     prefix: nil,
      #     params: [],
      #     hints: []
      #   },
      #   group_bys: [],
      #   havings: [],
      #   joins: [],
      #   limit: nil,
      #   lock: nil,
      #   offset: nil,
      #   order_bys: [],
      #   prefix: nil,
      #   preloads: [],
      #   select: %Ecto.Query.SelectExpr{
      #     expr: {:&, [], [0]},
      #     file: "/Users/q/Developer/chto/test/chto_test.exs",
      #     line: 110,
      #     fields: [
      #       {{:., [], [{:&, [], [0]}, :name]}, [], []},
      #       {{:., [], [{:&, [], [0]}, :user_id]}, [], []}
      #     ],
      #     params: nil,
      #     take: %{0 => {:map, [:name, :user_id]}},
      #     subqueries: [],
      #     aliases: %{}
      #   },
      #   sources: {{"events", Event, nil}},
      #   updates: [],
      #   wheres: [],
      #   windows: [],
      #   with_ctes: nil
      # }

      query = select(Event, [e], map(e, [:name, :user_id]))
      assert to_sql(query) == ~s[SELECT e0."name",e0."user_id" FROM "events" AS e0]

      # %{
      #   aliases: %{},
      #   assocs: [],
      #   combinations: [],
      #   distinct: nil,
      #   from: %Ecto.Query.FromExpr{
      #     source: {"events", Event},
      #     file: nil,
      #     line: nil,
      #     as: nil,
      #     prefix: nil,
      #     params: [],
      #     hints: []
      #   },
      #   group_bys: [],
      #   havings: [],
      #   joins: [],
      #   limit: %Ecto.Query.QueryExpr{
      #     expr: 1,
      #     file: "/Users/q/Developer/chto/test/chto_test.exs",
      #     line: 158,
      #     params: nil
      #   },
      #   lock: nil,
      #   offset: nil,
      #   order_bys: [],
      #   prefix: nil,
      #   preloads: [],
      #   select: %Ecto.Query.SelectExpr{
      #     expr: {{:., [type: :string], [{:&, [], [0]}, :name]}, [], []},
      #     file: "/Users/q/Developer/chto/test/chto_test.exs",
      #     line: 158,
      #     fields: [{{:., [type: :string], [{:&, [], [0]}, :name]}, [], []}],
      #     params: nil,
      #     take: %{},
      #     subqueries: [],
      #     aliases: %{}
      #   },
      #   sources: {{"events", Event, nil}},
      #   updates: [],
      #   wheres: [],
      #   windows: [],
      #   with_ctes: nil
      # }
      query = Event |> select([e], e.name) |> limit(1)
      assert to_sql(query) == ~s[SELECT e0."name" FROM "events" AS e0 LIMIT 1]

      # params

      # %{
      #   aliases: %{},
      #   assocs: [],
      #   combinations: [],
      #   distinct: nil,
      #   from: %Ecto.Query.FromExpr{
      #     source: {"events", Event},
      #     file: nil,
      #     line: nil,
      #     as: nil,
      #     prefix: nil,
      #     params: [],
      #     hints: []
      #   },
      #   group_bys: [],
      #   havings: [],
      #   joins: [],
      #   limit: nil,
      #   lock: nil,
      #   offset: nil,
      #   order_bys: [],
      #   prefix: nil,
      #   preloads: [],
      #   select: %Ecto.Query.SelectExpr{
      #     expr: {{:., [type: :integer], [{:&, [], [0]}, :user_id]}, [], []},
      #     file: "/Users/q/Developer/chto/test/chto_test.exs",
      #     line: 163,
      #     fields: [{{:., [type: :integer], [{:&, [], [0]}, :user_id]}, [], []}],
      #     params: nil,
      #     take: %{},
      #     subqueries: [],
      #     aliases: %{}
      #   },
      #   sources: {{"events", Event, nil}},
      #   updates: [],
      #   wheres: [
      #     %Ecto.Query.BooleanExpr{
      #       op: :and,
      #       expr: {:==, [],
      #       [{{:., [], [{:&, [], [0]}, :name]}, [], []}, {:^, [], [0]}]},
      #       file: "/Users/q/Developer/chto/test/chto_test.exs",
      #       line: 163,
      #       params: nil,
      #       subqueries: []
      #     }
      #   ],
      #   windows: [],
      #   with_ctes: nil
      # }
      name = "John"
      query = Event |> where(name: type(^name, :string)) |> select([e], e.user_id)

      # TODO
      assert to_sql(query) ==
               ~s[SELECT e0."user_id" FROM "events" AS e0 WHERE (e0."name" = {var:String})]

      # fragments

      # %{
      #   aliases: %{},
      #   assocs: [],
      #   combinations: [],
      #   distinct: nil,
      #   from: %Ecto.Query.FromExpr{
      #     source: {"events", Event},
      #     file: nil,
      #     line: nil,
      #     as: nil,
      #     prefix: nil,
      #     params: [],
      #     hints: []
      #   },
      #   group_bys: [],
      #   havings: [],
      #   joins: [],
      #   limit: nil,
      #   lock: nil,
      #   offset: nil,
      #   order_bys: [],
      #   prefix: nil,
      #   preloads: [],
      #   select: %Ecto.Query.SelectExpr{
      #     expr: {{:., [type: :integer], [{:&, [], [0]}, :user_id]}, [], []},
      #     file: "/Users/q/Developer/chto/test/chto_test.exs",
      #     line: 226,
      #     fields: [{{:., [type: :integer], [{:&, [], [0]}, :user_id]}, [], []}],
      #     params: nil,
      #     take: %{},
      #     subqueries: [],
      #     aliases: %{}
      #   },
      #   sources: {{"events", Event, nil}},
      #   updates: [],
      #   wheres: [
      #     %Ecto.Query.BooleanExpr{
      #       op: :and,
      #       expr: {:fragment, [],
      #        [
      #          raw: "name = ",
      #          expr: %Ecto.Query.Tagged{
      #            tag: :string,
      #            type: :string,
      #            value: {:^, [], [0]}
      #          },
      #          raw: ""
      #        ]},
      #       file: "/Users/q/Developer/chto/test/chto_test.exs",
      #       line: 225,
      #       params: nil,
      #       subqueries: []
      #     }
      #   ],
      #   windows: [],
      #   with_ctes: nil
      # }
      query =
        Event
        # TODO ** (Ecto.Query.CompileError) fragment(...) expects extra arguments in the same amount of question marks in string. It received 1 extra argument(s) but expected 0
        # |> where([t], fragment("name = {name:String}", %{"name" => "John"}))
        # |> where([e], fragment("name = {name:String}"))
        |> where([e], fragment("name = ?", type(^name, :string)))
        |> select([e], e.user_id)

      assert to_sql(query, inspect: []) ==
               ~s[SELECT e0."user_id" FROM "events" AS e0 WHERE (name = {var:String})]
    end
  end
end
