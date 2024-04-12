defmodule EctoCh.Test do
  use ExUnit.Case

  import Ecto.Query
  alias Ecto.Adapters.ClickHouse.Connection, as: SQL
  alias EctoClickHouse.Integration.Product

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
      tags = ["1", "2", "3"]
      date_range = %{first: ~D[2020-10-10], last: ~D[2021-01-01]}

      query =
        from e in "events",
          where: e.domain in ^domains,
          where: e.tags == ^tags,
          where: fragment("toDate(?)", e.inserted_at) >= ^date_range.first,
          where: fragment("toDate(?)", e.inserted_at) <= ^date_range.last,
          select: {
            fragment("countIf(? = 'pageview')", e.type),
            fragment("countIf(? != 'pageview')", e.type)
          }

      assert all(query) ==
               {
                 """
                 SELECT \
                 countIf(e0."type" = 'pageview'),\
                 countIf(e0."type" != 'pageview') \
                 FROM "events" AS e0 \
                 WHERE (\
                 e0."domain" IN ({$0:String},{$1:String})) AND \
                 (e0."tags" = {$2:Array(String)}) AND \
                 (toDate(e0."inserted_at") >= {$3:Date}) AND \
                 (toDate(e0."inserted_at") <= {$4:Date}\
                 )\
                 """,
                 [
                   "dummy.site",
                   "dummy2.site",
                   ["1", "2", "3"],
                   ~D[2020-10-10],
                   ~D[2021-01-01]
                 ]
               }
    end

    test "where array =" do
      domains = ["dummy.site", "dummy2.site"]
      query = "events" |> where(domains: ^domains) |> select([e], e.user_id)

      assert all(query) ==
               {~s[SELECT e0."user_id" FROM "events" AS e0 WHERE (e0."domains" = {$0:Array(String)})],
                [["dummy.site", "dummy2.site"]]}
    end

    test "where schema.array =" do
      tags = ["a", "b"]
      query = Product |> where(tags: ^tags) |> select([p], p.name)

      assert all(query) ==
               {~s[SELECT p0."name" FROM "products" AS p0 WHERE (p0."tags" = {$0:Array(String)})],
                [["a", "b"]]}
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

  test "to_sql" do
    user_id = 1
    query = "example" |> where([e], e.user_id == ^user_id) |> select([e], e.name)
    assert {sql, params} = Ecto.Integration.TestRepo.to_sql(:all, query)
    assert sql == ~s[SELECT e0."name" FROM "example" AS e0 WHERE (e0."user_id" = {$0:Int64})]
    assert params == [1]
  end

  test "to_sql interpolate" do
    interpolate = fn kind, query ->
      {_sql, params} = Ecto.Integration.TestRepo.to_sql(kind, query)
      {i_sql, i_params} = Ecto.Integration.TestRepo.to_sql(kind, query, interpolate: true)
      assert i_params == params
      i_sql
    end

    interpolate_all = fn query -> interpolate.(:all, query) end

    # string escape
    assert interpolate_all.("schema" |> where(foo: ^"'\\  ") |> select([], true)) ==
             "SELECT true FROM \"schema\" AS s0 WHERE (s0.\"foo\" = '''\\\\  ')"

    assert interpolate_all.("schema" |> where(foo: ^"'") |> select([], true)) ==
             "SELECT true FROM \"schema\" AS s0 WHERE (s0.\"foo\" = '''')"

    # literals
    assert interpolate_all.("schema" |> where(foo: ^true) |> select([], true)) ==
             ~s{SELECT true FROM "schema" AS s0 WHERE (s0."foo" = true)}

    assert interpolate_all.("schema" |> where(foo: ^false) |> select([], true)) ==
             ~s{SELECT true FROM "schema" AS s0 WHERE (s0."foo" = false)}

    assert interpolate_all.("schema" |> where(foo: ^"abc") |> select([], true)) ==
             ~s{SELECT true FROM "schema" AS s0 WHERE (s0."foo" = 'abc')}

    assert interpolate_all.("schema" |> where(foo: ^123) |> select([], true)) ==
             ~s{SELECT true FROM "schema" AS s0 WHERE (s0."foo" = 123)}

    assert interpolate_all.("schema" |> where(foo: ^123.0) |> select([], true)) ==
             ~s{SELECT true FROM "schema" AS s0 WHERE (s0."foo" = 123.0)}

    assert interpolate_all.(
             "schema"
             |> where(fragment("? = ?", literal(^"y"), ^"Main"))
             |> select([], true)
           ) == ~s|SELECT true FROM "schema" AS s0 WHERE ("y" = 'Main')|

    # tagged type
    assert interpolate_all.(
             "schema"
             |> select([], type(^"601d74e4-a8d3-4b6e-8365-eddb4c893327", Ecto.UUID))
           ) ==
             ~s[SELECT CAST('601d74e4-a8d3-4b6e-8365-eddb4c893327' AS UUID) FROM "schema" AS s0]

    # in expression
    assert interpolate_all.("schema" |> select([e], 1 in [^1, e.x, ^3])) ==
             ~s[SELECT 1 IN (1,s0."x",3) FROM "schema" AS s0]

    assert interpolate_all.("schema" |> select([e], 1 in ^[])) == ~s[SELECT 0 FROM "schema" AS s0]

    assert interpolate_all.("schema" |> select([e], 1 in ^[1, 2, 3])) ==
             ~s[SELECT 1 IN (1,2,3) FROM "schema" AS s0]

    assert interpolate_all.("schema" |> select([e], 1 in [1, ^2, 3])) ==
             ~s[SELECT 1 IN (1,2,3) FROM "schema" AS s0]

    assert interpolate_all.("schema" |> select([e], e.x == ^0 or e.x in ^[1, 2, 3] or e.x == ^4)) ==
             ~s[SELECT ((s0."x" = 0) OR (s0."x" IN (1,2,3))) OR (s0."x" = 4) FROM "schema" AS s0]

    assert interpolate_all.("schema" |> select([e], e in [1, 2, 3])) ==
             ~s|SELECT s0 IN (1,2,3) FROM "schema" AS s0|

    # interpolated values
    cte1 =
      "schema1"
      |> select([m], %{id: m.id, smth: ^true})
      |> where([], fragment("?", ^1))

    union =
      "schema1"
      |> select([m], {m.id, ^true})
      |> where([], fragment("?", ^5))

    union_all =
      "schema2"
      |> select([m], {m.id, ^false})
      |> where([], fragment("?", ^6))

    query =
      "schema"
      |> with_cte("cte1", as: ^cte1)
      |> with_cte("cte2", as: fragment("SELECT * FROM schema WHERE ?", ^2))
      |> select([m], {m.id, ^0})
      |> join(:inner, [], "schema2", on: fragment("?", ^true))
      |> join(:inner, [], "schema2", on: fragment("?", ^false))
      |> where([], fragment("?", ^true))
      |> where([], fragment("?", ^false))
      |> having([], fragment("?", ^true))
      |> having([], fragment("?", ^false))
      |> group_by([], fragment("?", ^3))
      |> group_by([], fragment("?", ^4))
      |> union(^union)
      |> union_all(^union_all)
      |> order_by([], fragment("?", ^7))
      |> limit([], ^8)
      |> offset([], ^9)

    assert interpolate_all.(query) ==
             """
             WITH \
             "cte1" AS (\
             SELECT ss0."id" AS "id",true AS "smth" FROM "schema1" AS ss0 \
             WHERE (1)\
             ),\
             "cte2" AS (\
             SELECT * FROM schema WHERE 2\
             ) \
             SELECT s0."id",0 FROM "schema" AS s0 \
             INNER JOIN "schema2" AS s1 ON true \
             INNER JOIN "schema2" AS s2 ON false \
             WHERE (true) AND (false) \
             GROUP BY 3,4 \
             HAVING (true) AND (false) \
             ORDER BY 7 \
             LIMIT 8 \
             OFFSET 9 \
             UNION \
             (SELECT s0."id",true FROM "schema1" AS s0 \
             WHERE (5)) \
             UNION ALL \
             (SELECT s0."id",false FROM "schema2" AS s0 \
             WHERE (6))\
             """

    # delete all
    assert interpolate.(:delete_all, from(e in "schema", where: e.x == ^123)) ==
             ~s[DELETE FROM "schema" WHERE ("x" = 123)]

    # join with subquery
    posts =
      "posts"
      |> where(title: ^"hello")
      |> select([r], %{x: r.x, y: r.y})
      |> subquery()

    query =
      "comments"
      |> join(:inner, [c], p in subquery(posts), on: true)
      |> select([_, p], p.x)

    assert interpolate_all.(query) ==
             """
             SELECT s1."x" FROM "comments" AS c0 \
             INNER JOIN (\
             SELECT sp0."x" AS "x",sp0."y" AS "y" \
             FROM "posts" AS sp0 \
             WHERE (sp0."title" = 'hello')\
             ) AS s1 ON 1\
             """

    posts =
      "posts"
      |> where(title: ^"hello")
      |> select([r], %{x: r.x, z: r.y})
      |> subquery()

    query =
      "comments"
      |> join(:inner, [c], p in subquery(posts), on: true)
      |> select([_, p], p)

    assert interpolate_all.(query) ==
             """
             SELECT s1."x",s1."z" FROM "comments" AS c0 \
             INNER JOIN (\
             SELECT sp0."x" AS "x",sp0."y" AS "z" \
             FROM "posts" AS sp0 \
             WHERE (sp0."title" = 'hello')\
             ) AS s1 ON 1\
             """

    # join with fragment
    query =
      "schema"
      |> join(
        :inner,
        [p],
        q in fragment(
          "SELECT * FROM schema2 AS s2 WHERE s2.id = ? AND s2.field = ?",
          p.x,
          ^10
        ),
        on: true
      )
      |> select([p], {p.id, ^0})
      |> where([p], p.id > 0 and p.id < ^100)

    assert interpolate_all.(query) ==
             """
             SELECT s0."id",0 \
             FROM "schema" AS s0 \
             INNER JOIN \
             (\
             SELECT * \
             FROM schema2 AS s2 \
             WHERE s2.id = s0."x" AND s2.field = 10\
             ) AS f1 ON 1 \
             WHERE ((s0."id" > 0) AND (s0."id" < 100))\
             """

    # arrays
    query =
      "schema"
      |> select([], fragment("?", ^[1, 2, 3]))

    assert interpolate_all.(query) == ~s{SELECT [1,2,3] FROM "schema" AS s0}

    # maps
    assert interpolate_all.(select("schema", [], fragment("?[site_id]", ^%{}))) ==
             ~s|SELECT {}[site_id] FROM "schema" AS s0|

    assert interpolate_all.(
             select("schema", [], fragment("?[site_id]", ^%{1 => "UTC", 2 => "Europe/Vienna"}))
           ) ==
             ~s|SELECT {1:'UTC',2:'Europe/Vienna'}[site_id] FROM "schema" AS s0|
  end
end
