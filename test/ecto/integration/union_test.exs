defmodule Ecto.Integration.UnionTest do
  use Ecto.Integration.Case
  import Ecto.Query

  alias Ecto.Integration.TestRepo
  alias Ecto.Integration.Post

  test "union & ordering" do
    TestRepo.insert!(%Post{title: "hello", counter: 1, public: true})
    TestRepo.insert!(%Post{title: "morning", counter: 2, public: true})
    TestRepo.insert!(%Post{title: "bye", counter: 3, public: false})

    other =
      from(
        p in Post,
        where: p.public,
        order_by: p.counter,
        limit: 1,
        select: p.title
      )

    data =
      TestRepo.all(
        from(
          p in Post,
          union_all: ^other,
          where: not p.public,
          order_by: p.counter,
          select: p.title
        )
      )

    assert Enum.sort(data) == Enum.sort(["bye", "hello"])
  end

  test "union & params" do
    TestRepo.insert!(%Post{title: "hello", counter: 1, public: true})
    TestRepo.insert!(%Post{title: "morning", counter: 2, public: true})
    TestRepo.insert!(%Post{title: "bye", counter: 3, public: false})

    hello_and_morning =
      from p in Post,
        where: p.public == ^true,
        order_by: :counter,
        limit: ^2,
        select: p.title

    morning_1 =
      from p in Post,
        where: p.public == ^true,
        order_by: [desc: :counter],
        limit: ^1,
        select: p.title

    morning_2 =
      from p in Post,
        where: p.public == ^true,
        order_by: :counter,
        offset: ^1,
        limit: ^1,
        select: p.title

    bye =
      from p in Post,
        where: p.public == ^false,
        order_by: :counter,
        limit: ^1,
        select: p.title

    query =
      hello_and_morning
      |> union_all(^morning_1)
      |> union_all(^bye)
      |> union_all(^morning_2)

    {sql, params} = TestRepo.to_sql(:all, query)

    # ensures param idx=8 is 2 (from limit: ^2 above)
    assert Enum.at(params, 8) == 2

    assert sql == """
           SELECT p0."title" FROM "posts" AS p0 \
           WHERE p0."public" = {$0:Bool} \
           ORDER BY p0."counter" \
           LIMIT {$8:Int64} \
           UNION ALL \
           (\
           SELECT p0."title" FROM "posts" AS p0 \
           WHERE p0."public" = {$1:Bool} \
           ORDER BY p0."counter" DESC \
           LIMIT {$2:Int64}\
           ) \
           UNION ALL \
           (\
           SELECT p0."title" FROM "posts" AS p0 \
           WHERE p0."public" = {$3:Bool} \
           ORDER BY p0."counter" \
           LIMIT {$4:Int64}\
           ) \
           UNION ALL \
           (\
           SELECT p0."title" FROM "posts" AS p0 \
           WHERE p0."public" = {$5:Bool} \
           ORDER BY p0."counter" \
           LIMIT {$6:Int64} \
           OFFSET {$7:Int64}\
           )\
           """

    actual = TestRepo.all(query)
    expected = ["bye", "morning", "hello", "morning", "morning"]

    assert Enum.sort(actual) == Enum.sort(expected)
  end
end
