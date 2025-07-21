defmodule Ecto.Integration.UpdateTest do
  use Ecto.Integration.Case, async: true
  import Ecto.Query
  alias Ecto.Integration.TestRepo

  @moduletag :update

  # https://clickhouse.com/docs/sql-reference/statements/update

  # TODO please open an issue if you need this functionality!
  @tag :skip
  test "ON CLUSTER"

  @tag :skip
  test "IN PARTITION"

  defp to_sql(query) do
    TestRepo.to_sql(:update_all, query)
  end

  describe "to_sql/2" do
    test "set" do
      assert to_sql(
               "events"
               |> where(name: "hello")
               |> update(set: [i: 1])
             ) == {~s[UPDATE "events" SET "i"=1 WHERE ("name" = 'hello')], []}

      assert to_sql(
               "events"
               |> where(name: "hello")
               |> update(set: [i: ^1])
             ) == {
               ~s[UPDATE "events" SET "i"={$0:Int64} WHERE ("name" = 'hello')],
               [1]
             }

      assert to_sql(
               "events"
               |> where(name: ^"hello")
               |> update(set: [i: ^1])
             ) == {
               ~s[UPDATE "events" SET "i"={$0:Int64} WHERE ("name" = {$1:String})],
               [1, "hello"]
             }

      assert to_sql(
               "events"
               |> where(name: ^"hello")
               |> update(set: [i: 1])
             ) == {
               ~s[UPDATE "events" SET "i"=1 WHERE ("name" = {$0:String})],
               ["hello"]
             }
    end

    test "inc" do
      assert to_sql(
               "events"
               |> where(name: "hello")
               |> update(inc: [i: 1])
             ) == {
               ~s[UPDATE "events" SET "i"="i"+1 WHERE ("name" = 'hello')],
               []
             }

      assert to_sql(
               "events"
               |> where(name: ^"hello")
               |> update(inc: [i: ^1])
             ) ==
               {~s[UPDATE "events" SET "i"="i"+{$0:Int64} WHERE ("name" = {$1:String})],
                [1, "hello"]}

      assert to_sql(
               "events"
               |> where(name: "hello")
               |> update(inc: [i: -1])
             ) == {~s[UPDATE "events" SET "i"="i"+-1 WHERE ("name" = 'hello')], []}

      assert to_sql(
               "events"
               |> where(name: ^"hello")
               |> update(inc: [i: ^(-1)])
             ) ==
               {~s[UPDATE "events" SET "i"="i"+{$0:Int64} WHERE ("name" = {$1:String})],
                [-1, "hello"]}
    end

    test "push" do
      assert to_sql(
               "events"
               |> where(name: "hello")
               |> update(push: [i: 1])
             ) == {
               ~s[UPDATE "events" SET "i"=arrayPushBack("i",1) WHERE ("name" = 'hello')],
               []
             }

      assert to_sql(
               "events"
               |> where(name: "hello")
               |> update(push: [i: ^1])
             ) == {
               ~s[UPDATE "events" SET "i"=arrayPushBack("i",{$0:Int64}) WHERE ("name" = 'hello')],
               [1]
             }

      assert to_sql(
               "events"
               |> where(name: ^"hello")
               |> update(push: [i: ^1])
             ) == {
               ~s[UPDATE "events" SET "i"=arrayPushBack("i",{$0:Int64}) WHERE ("name" = {$1:String})],
               [1, "hello"]
             }

      assert to_sql(
               "events"
               |> where(name: ^"hello")
               |> update(push: [i: 1])
             ) == {
               ~s[UPDATE "events" SET "i"=arrayPushBack("i",1) WHERE ("name" = {$0:String})],
               ["hello"]
             }
    end

    test "pull" do
      assert to_sql(
               "events"
               |> where(name: "hello")
               |> update(pull: [i: 1])
             ) == {
               ~s[UPDATE "events" SET "i"=arrayFilter(x->x!=1,"i") WHERE ("name" = 'hello')],
               []
             }

      assert to_sql(
               "events"
               |> where(name: "hello")
               |> update(pull: [i: ^1])
             ) == {
               ~s[UPDATE "events" SET "i"=arrayFilter(x->x!={$0:Int64},"i") WHERE ("name" = 'hello')],
               [1]
             }

      assert to_sql(
               "events"
               |> where(name: ^"hello")
               |> update(pull: [i: ^1])
             ) == {
               ~s[UPDATE "events" SET "i"=arrayFilter(x->x!={$0:Int64},"i") WHERE ("name" = {$1:String})],
               [1, "hello"]
             }

      assert to_sql(
               "events"
               |> where(name: ^"hello")
               |> update(pull: [i: 1])
             ) == {
               ~s[UPDATE "events" SET "i"=arrayFilter(x->x!=1,"i") WHERE ("name" = {$0:String})],
               ["hello"]
             }
    end
  end

  describe "update_all/2" do
    test "updates all rows if no where is provided" do
      TestRepo.query!("""
      CREATE TABLE update_no_where_test(
        s String,
        i UInt8
      )
      ENGINE MergeTree
      ORDER BY tuple()
      SETTINGS
        enable_block_number_column = true,
        enable_block_offset_column = true
      """)

      on_exit(fn -> TestRepo.query!("DROP TABLE update_no_where_test") end)
      TestRepo.query!("INSERT INTO update_no_where_test VALUES ('Hello', 0), ('World', 0)")

      assert "update_no_where_test"
             |> select([t], map(t, [:s, :i]))
             |> TestRepo.all() == [
               %{s: "Hello", i: 0},
               %{s: "World", i: 0}
             ]

      TestRepo.update_all("update_no_where_test", [set: [i: 10]],
        settings: [allow_experimental_lightweight_update: 1]
      )

      assert "update_no_where_test"
             |> select([t], map(t, [:s, :i]))
             |> TestRepo.all() == [
               %{s: "Hello", i: 10},
               %{s: "World", i: 10}
             ]
    end

    test "set/inc" do
      TestRepo.query!("""
      CREATE TABLE update_inc_test(
        s String,
        i UInt8
      )
      ENGINE MergeTree
      ORDER BY tuple()
      SETTINGS
        enable_block_number_column = true,
        enable_block_offset_column = true
      """)

      on_exit(fn -> TestRepo.query!("DROP TABLE update_inc_test") end)
      TestRepo.query!("INSERT INTO update_inc_test VALUES ('Hello', 0), ('World', 0)")

      assert "update_inc_test"
             |> select([t], map(t, [:s, :i]))
             |> TestRepo.all() == [
               %{s: "Hello", i: 0},
               %{s: "World", i: 0}
             ]

      opts = [settings: [allow_experimental_lightweight_update: 1]]

      "update_inc_test"
      |> where(s: "Hello")
      |> update(set: [i: 1])
      |> TestRepo.update_all(_updates = [], opts)

      "update_inc_test"
      |> where(s: ^"World")
      |> TestRepo.update_all([set: [i: 2]], opts)

      assert "update_inc_test"
             |> select([t], map(t, [:s, :i]))
             |> TestRepo.all() == [
               %{s: "Hello", i: 1},
               %{s: "World", i: 2}
             ]

      "update_inc_test"
      |> where(s: "Hello")
      |> update(inc: [i: ^1])
      |> TestRepo.update_all(_updates = [], opts)

      "update_inc_test"
      |> where(s: ^"World")
      |> TestRepo.update_all([inc: [i: 2]], opts)

      assert "update_inc_test"
             |> select([t], map(t, [:s, :i]))
             |> TestRepo.all() == [
               %{s: "Hello", i: 2},
               %{s: "World", i: 4}
             ]

      "update_inc_test"
      |> where(s: "Hello")
      |> TestRepo.update_all([inc: [i: -1]], opts)

      "update_inc_test"
      |> where(s: ^"World")
      |> update(inc: [i: ^(-2)])
      |> TestRepo.update_all(_updates = [], opts)

      assert "update_inc_test"
             |> select([t], map(t, [:s, :i]))
             |> TestRepo.all() == [
               %{s: "Hello", i: 1},
               %{s: "World", i: 2}
             ]
    end

    test "push/pull" do
      TestRepo.query!("""
      CREATE TABLE update_arrays_test(
        s String,
        arr Array(UInt8)
      )
      ENGINE MergeTree
      ORDER BY tuple()
      SETTINGS
        enable_block_number_column = true,
        enable_block_offset_column = true
      """)

      on_exit(fn -> TestRepo.query!("DROP TABLE update_arrays_test") end)

      TestRepo.query!(
        "INSERT INTO update_arrays_test VALUES ('Hello', [1,2]), ('World', [3,4,5]), ('Goodbye', [])"
      )

      assert "update_arrays_test"
             |> select([t], map(t, [:s, :arr]))
             |> TestRepo.all() == [
               %{s: "Hello", arr: [1, 2]},
               %{s: "World", arr: [3, 4, 5]},
               %{s: "Goodbye", arr: []}
             ]

      opts = [settings: [allow_experimental_lightweight_update: 1]]

      "update_arrays_test"
      |> where(s: "Goodbye")
      |> TestRepo.update_all([push: [arr: 6]], opts)

      "update_arrays_test"
      |> update(push: [arr: 7])
      |> where(s: ^"World")
      |> TestRepo.update_all(_updates = [], opts)

      "update_arrays_test"
      |> where(s: "Hello")
      |> TestRepo.update_all([pull: [arr: 1]], opts)

      "update_arrays_test"
      |> update(pull: [arr: ^4])
      |> where(s: ^"World")
      |> TestRepo.update_all(_updates = [], opts)

      assert "update_arrays_test"
             |> select([t], map(t, [:s, :arr]))
             |> TestRepo.all() == [
               %{s: "Hello", arr: [2]},
               %{s: "World", arr: [3, 5, 7]},
               %{s: "Goodbye", arr: [6]}
             ]
    end
  end
end
