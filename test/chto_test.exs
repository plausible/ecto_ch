defmodule ChtoTest do
  use ExUnit.Case
  import Ecto.Query
  alias Ecto.Adapters.ClickHouse.Connection, as: SQL

  describe "all" do
    test "select one column" do
      query = select("events", [e], e.name)
      # TODO drop AS?
      assert all(query) ==
               ~s[SELECT e0."name" FROM "events" AS e0 FORMAT RowBinaryWithNamesAndTypes]
    end

    test "select two columns" do
      query = select("events", [e], map(e, [:name, :user_id]))

      assert all(query) ==
               ~s[SELECT e0."name",e0."user_id" FROM "events" AS e0 FORMAT RowBinaryWithNamesAndTypes]
    end

    test "limit" do
      query = "events" |> select([e], e.name) |> limit(1)

      assert all(query) ==
               ~s[SELECT e0."name" FROM "events" AS e0 LIMIT 1 FORMAT RowBinaryWithNamesAndTypes]
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
               ~s[SELECT e0."user_id" FROM "events" AS e0 WHERE (e0."name" = {$0:String}) AND (e0."user_id" > {$1:Int64}) FORMAT RowBinaryWithNamesAndTypes]
    end

    test "where with fragment" do
      name = "John"

      query =
        "events"
        |> where([e], fragment("name = ?", ^name))
        |> select([e], e.user_id)

      assert all(query) ==
               ~s[SELECT e0."user_id" FROM "events" AS e0 WHERE (name = {$0:String}) FORMAT RowBinaryWithNamesAndTypes]
    end
  end

  @tag skip: true
  test "dev" do
    user_id = 10

    query =
      "users"
      |> where(id: ^user_id)
      |> select([e], e.name)

    assert {query, params, key} = Ecto.Query.Planner.plan(query, :all, Ecto.Adapters.ClickHouse)
    assert params == [{10, 10}]

    assert key == [
             :all,
             {:where,
              [and: {:==, [], [{{:., [], [{:&, [], [0]}, :id]}, [], []}, {:^, [], [0]}]}]},
             {:from, {{"users", nil}, nil}, []},
             {:select, {{:., [], [{:&, [], [0]}, :name]}, [], []}}
           ]

    assert query == %Ecto.Query{
             aliases: %{},
             assocs: [],
             combinations: [],
             distinct: nil,
             from: %Ecto.Query.FromExpr{
               source: {"users", nil},
               file: nil,
               line: nil,
               as: nil,
               prefix: nil,
               params: [],
               hints: []
             },
             group_bys: [],
             havings: [],
             joins: [],
             limit: nil,
             lock: nil,
             offset: nil,
             order_bys: [],
             prefix: nil,
             preloads: [],
             select: %Ecto.Query.SelectExpr{
               expr: {{:., [], [{:&, [], [0]}, :name]}, [], []},
               file: "/Users/q/Developer/chto/test/chto_test.exs",
               line: 56,
               fields: nil,
               params: [],
               take: %{},
               subqueries: [],
               aliases: %{}
             },
             sources: {{"users", nil, nil}},
             updates: [],
             wheres: [
               %Ecto.Query.BooleanExpr{
                 op: :and,
                 expr: {:==, [], [{{:., [], [{:&, [], [0]}, :id]}, [], []}, {:^, [], [0]}]},
                 file: "/Users/q/Developer/chto/test/chto_test.exs",
                 line: 55,
                 params: [{10, {0, :id}}],
                 subqueries: []
               }
             ],
             windows: [],
             with_ctes: nil
           }

    assert {query, select} =
             Ecto.Query.Planner.normalize(query, :all, Ecto.Adapters.ClickHouse, _counter = 0)

    assert select == %{
             assocs: [],
             from: :none,
             postprocess: {:value, :any},
             preprocess: [],
             take: []
           }

    assert query == %Ecto.Query{
             aliases: %{},
             assocs: [],
             combinations: [],
             distinct: nil,
             from: %Ecto.Query.FromExpr{
               source: {"users", nil},
               file: nil,
               line: nil,
               as: nil,
               prefix: nil,
               params: [],
               hints: []
             },
             group_bys: [],
             havings: [],
             joins: [],
             limit: nil,
             lock: nil,
             offset: nil,
             order_bys: [],
             prefix: nil,
             preloads: [],
             select: %Ecto.Query.SelectExpr{
               expr: {{:., [{:type, :any}], [{:&, [], [0]}, :name]}, [], []},
               file: "/Users/q/Developer/chto/test/chto_test.exs",
               line: 56,
               fields: [{{:., [type: :any], [{:&, [], [0]}, :name]}, [], []}],
               params: nil,
               take: %{},
               subqueries: [],
               aliases: %{}
             },
             sources: {{"users", nil, nil}},
             updates: [],
             wheres: [
               %Ecto.Query.BooleanExpr{
                 op: :and,
                 expr: {:==, [], [{{:., [], [{:&, [], [0]}, :id]}, [], []}, {:^, [], [0]}]},
                 file: "/Users/q/Developer/chto/test/chto_test.exs",
                 line: 55,
                 params: nil,
                 subqueries: []
               }
             ],
             windows: [],
             with_ctes: nil
           }
  end

  defp all(query) do
    to_sql(query, fn query, params -> SQL.all(query, params) end)
  end

  defp to_sql(query, f) do
    {query, params, _key} = Ecto.Query.Planner.plan(query, :all, Ecto.Adapters.ClickHouse)
    {query, _} = Ecto.Query.Planner.normalize(query, :all, Ecto.Adapters.ClickHouse, _counter = 0)
    {dump_params, _} = Enum.unzip(params)
    IO.iodata_to_binary(f.(query, dump_params))
  end

  def i(query) do
    IO.inspect(Map.from_struct(query), limit: :infinity)
  end
end
