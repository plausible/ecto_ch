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

      # TODO =ANY() vs in
      assert all(query) ==
               {"SELECT countIf(e0.\"name\" = 'pageview'),countIf(e0.\"name\" != 'pageview') FROM \"events\" AS e0 WHERE (e0.\"domain\" IN {$0:Array(String)}) AND (toDate(e0.\"timestamp\") >= {$1:Date}) AND (toDate(e0.\"timestamp\") <= {$2:Date})",
                [["dummy.site", "dummy2.site"], ~D[2020-10-10], ~D[2021-01-01]]}
    end
  end

  test "dev" do
    user_ids = [10, 20]
    names = ["John", "Elliot"]

    query =
      "users"
      |> where([e], e.user_ids == ^user_ids)
      |> where([e], e.name in ^names)
      |> select([e], e.name)

    assert all(query) == nil

    # assert {query, params, key} = Ecto.Query.Planner.plan(query, :all, Ecto.Adapters.ClickHouse)
    # assert params == [{10, 10}]

    # assert key == [
    #          :all,
    #          {:where,
    #           [and: {:==, [], [{{:., [], [{:&, [], [0]}, :id]}, [], []}, {:^, [], [0]}]}]},
    #          {:from, {{"users", nil}, nil}, []},
    #          {:select, {{:., [], [{:&, [], [0]}, :name]}, [], []}}
    #        ]

    # assert query == %Ecto.Query{
    #          aliases: %{},
    #          assocs: [],
    #          combinations: [],
    #          distinct: nil,
    #          from: %Ecto.Query.FromExpr{
    #            source: {"users", nil},
    #            file: nil,
    #            line: nil,
    #            as: nil,
    #            prefix: nil,
    #            params: [],
    #            hints: []
    #          },
    #          group_bys: [],
    #          havings: [],
    #          joins: [],
    #          limit: nil,
    #          lock: nil,
    #          offset: nil,
    #          order_bys: [],
    #          prefix: nil,
    #          preloads: [],
    #          select: %Ecto.Query.SelectExpr{
    #            expr: {{:., [], [{:&, [], [0]}, :name]}, [], []},
    #            file: "/Users/q/Developer/chto/test/chto_test.exs",
    #            line: 56,
    #            fields: nil,
    #            params: [],
    #            take: %{},
    #            subqueries: [],
    #            aliases: %{}
    #          },
    #          sources: {{"users", nil, nil}},
    #          updates: [],
    #          wheres: [
    #            %Ecto.Query.BooleanExpr{
    #              op: :and,
    #              expr: {:==, [], [{{:., [], [{:&, [], [0]}, :id]}, [], []}, {:^, [], [0]}]},
    #              file: "/Users/q/Developer/chto/test/chto_test.exs",
    #              line: 55,
    #              params: [{10, {0, :id}}],
    #              subqueries: []
    #            }
    #          ],
    #          windows: [],
    #          with_ctes: nil
    #        }

    # assert {query, select} =
    #          Ecto.Query.Planner.normalize(query, :all, Ecto.Adapters.ClickHouse, _counter = 0)

    # assert select == %{
    #          assocs: [],
    #          from: :none,
    #          postprocess: {:value, :any},
    #          preprocess: [],
    #          take: []
    #        }

    # assert query == %Ecto.Query{
    #          aliases: %{},
    #          assocs: [],
    #          combinations: [],
    #          distinct: nil,
    #          from: %Ecto.Query.FromExpr{
    #            source: {"users", nil},
    #            file: nil,
    #            line: nil,
    #            as: nil,
    #            prefix: nil,
    #            params: [],
    #            hints: []
    #          },
    #          group_bys: [],
    #          havings: [],
    #          joins: [],
    #          limit: nil,
    #          lock: nil,
    #          offset: nil,
    #          order_bys: [],
    #          prefix: nil,
    #          preloads: [],
    #          select: %Ecto.Query.SelectExpr{
    #            expr: {{:., [{:type, :any}], [{:&, [], [0]}, :name]}, [], []},
    #            file: "/Users/q/Developer/chto/test/chto_test.exs",
    #            line: 56,
    #            fields: [{{:., [type: :any], [{:&, [], [0]}, :name]}, [], []}],
    #            params: nil,
    #            take: %{},
    #            subqueries: [],
    #            aliases: %{}
    #          },
    #          sources: {{"users", nil, nil}},
    #          updates: [],
    #          wheres: [
    #            %Ecto.Query.BooleanExpr{
    #              op: :and,
    #              expr: {:==, [], [{{:., [], [{:&, [], [0]}, :id]}, [], []}, {:^, [], [0]}]},
    #              file: "/Users/q/Developer/chto/test/chto_test.exs",
    #              line: 55,
    #              params: nil,
    #              subqueries: []
    #            }
    #          ],
    #          windows: [],
    #          with_ctes: nil
    #        }
  end

  defp all(query) do
    to_sql(query, fn query, params -> SQL.all(query, params) end)
  end

  defp to_sql(query, f) do
    {query, params, _key} = Ecto.Query.Planner.plan(query, :all, Ecto.Adapters.ClickHouse)
    {query, _} = Ecto.Query.Planner.normalize(query, :all, Ecto.Adapters.ClickHouse, _counter = 0)
    {dump_params, _} = Enum.unzip(params)
    {sql, params} = f.(query, dump_params)
    {IO.iodata_to_binary(sql), params}
  end

  def i(query) do
    IO.inspect(Map.from_struct(query), limit: :infinity)
  end
end
