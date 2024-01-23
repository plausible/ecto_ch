defmodule Ecto.Integration.AggregateFunctionTypeTest do
  use Ecto.Integration.Case
  import Ecto.Query
  alias Ecto.Integration.TestRepo

  # some tests are based on https://kb.altinity.com/altinity-kb-schema-design/ingestion-aggregate-function/

  defmacrop argMaxMerge(field) do
    quote do
      fragment("argMaxMerge(?)", unquote(field))
    end
  end

  describe "ephemeral column" do
    setup do
      TestRepo.query!("""
      CREATE TABLE agg_test_users (
        uid Int16,
        updated SimpleAggregateFunction(max, DateTime),
        name_stub String Ephemeral,
        name AggregateFunction(argMax, String, DateTime) DEFAULT arrayReduce('argMaxState', [name_stub], [updated])
      ) ENGINE AggregatingMergeTree ORDER BY uid
      """)

      on_exit(fn -> TestRepo.query!("DROP TABLE agg_test_users") end)
    end

    test "schemaless" do
      assert {2, _} =
               TestRepo.insert_all(
                 "agg_test_users",
                 [
                   [uid: 1231, updated: ~N[2020-01-02 00:00:00], name_stub: "Jane"],
                   [uid: 1231, updated: ~N[2020-01-01 00:00:00], name_stub: "John"]
                 ],
                 types: [
                   uid: "Int16",
                   updated: "SimpleAggregateFunction(max, DateTime)",
                   name_stub: "String"
                 ]
               )

      assert "agg_test_users"
             |> select([u], %{uid: u.uid, updated: max(u.updated), name: argMaxMerge(u.name)})
             |> group_by([u], u.uid)
             |> TestRepo.all() == [%{uid: 1231, updated: ~N[2020-01-02 00:00:00], name: "Jane"}]
    end

    defmodule UserEphemeral do
      use Ecto.Schema

      @primary_key false
      schema "agg_test_users" do
        field :uid, Ch, type: "Int16"
        field :updated, Ch, type: "SimpleAggregateFunction(max, DateTime)"
        field :name_stub, :string
      end
    end

    test "schemafull" do
      assert {2, _} =
               TestRepo.insert_all(
                 UserEphemeral,
                 [
                   [uid: 1231, updated: ~N[2020-01-02 00:00:00], name_stub: "Jane"],
                   [uid: 1231, updated: ~N[2020-01-01 00:00:00], name_stub: "John"]
                 ]
               )

      assert "agg_test_users"
             |> select([u], %{uid: u.uid, updated: max(u.updated), name: argMaxMerge(u.name)})
             |> group_by([u], u.uid)
             |> TestRepo.all() == [%{uid: 1231, updated: ~N[2020-01-02 00:00:00], name: "Jane"}]
    end
  end

  describe "input function" do
    setup do
      TestRepo.query!("""
      CREATE TABLE agg_test_users (
        uid Int16,
        updated SimpleAggregateFunction(max, DateTime),
        name AggregateFunction(argMax, String, DateTime)
      ) ENGINE AggregatingMergeTree ORDER BY uid
      """)

      on_exit(fn -> TestRepo.query!("DROP TABLE agg_test_users") end)
    end

    @tag :skip
    test "schemaless" do
      rows = [
        [uid: 1231, updated: ~N[2020-01-02 00:00:00], name: "Jane"],
        [uid: 1231, updated: ~N[2020-01-01 00:00:00], name: "John"]
      ]

      input =
        from i in fragment("input('uid Int16, updated DateTime, name String')"),
          select: %{
            uid: i.uid,
            updated: i.updated,
            name: fragment("arrayReduce('argMaxState', [?], [?])", i.name, i.updated)
          }

      assert {2, _} =
               TestRepo.checkout(fn ->
                 Enum.into(rows, TestRepo.stream(input))
               end)

      assert "agg_test_users"
             |> select([u], %{uid: u.uid, updated: max(u.updated), name: argMaxMerge(u.name)})
             |> group_by([u], u.uid)
             |> TestRepo.all() == [%{uid: 1231, updated: ~N[2020-01-02 00:00:00], name: "Jane"}]
    end

    defmodule UserInput do
      use Ecto.Schema

      @primary_key false
      schema "agg_test_users" do
        field :uid, Ch, type: "Int16"
        field :updated, :naive_datetime
        field :name, :string
      end
    end

    @tag :skip
    test "schemafull" do
      # input =
      #   from i in input(UserInput),
      #     # TODO
      #     # select_merge: %{
      #     #   name: fragment("arrayReduce('argMaxState', [?], [?])", i.name, i.updated)
      #     # }
      #     select: %{
      #       uid: i.uid,
      #       updated: i.updated,
      #       name: fragment("arrayReduce('argMaxState', [?], [?])", i.name, i.updated)
      #     }

      # rows = [
      #   [uid: 1231, updated: ~N[2020-01-02 00:00:00], name: "Jane"],
      #   [uid: 1231, updated: ~N[2020-01-01 00:00:00], name: "John"]
      # ]

      # assert {2, _} = TestRepo.insert_all(UserInput, rows, input: input)

      assert "agg_test_users"
             |> select([u], %{uid: u.uid, updated: max(u.updated), name: argMaxMerge(u.name)})
             |> group_by([u], u.uid)
             |> TestRepo.all() == [%{uid: 1231, updated: ~N[2020-01-02 00:00:00], name: "Jane"}]
    end
  end

  describe "materialized view and null engine" do
    setup do
      TestRepo.query!("""
      CREATE TABLE agg_test_users (
        uid Int16,
        updated SimpleAggregateFunction(max, DateTime),
        name AggregateFunction(argMax, String, DateTime)
      ) ENGINE AggregatingMergeTree ORDER BY uid
      """)

      on_exit(fn -> TestRepo.query!("DROP TABLE agg_test_users") end)

      TestRepo.query!("""
      CREATE TABLE agg_test_users_null (
        uid Int16,
        updated DateTime,
        name String
      ) ENGINE Null
      """)

      on_exit(fn -> TestRepo.query!("DROP TABLE agg_test_users_null") end)

      TestRepo.query!("""
      CREATE MATERIALIZED VIEW agg_test_users_mv TO agg_test_users AS
        SELECT uid, updated, arrayReduce('argMaxState', [name], [updated]) name
        FROM agg_test_users_null
      """)

      on_exit(fn -> TestRepo.query!("DROP VIEW agg_test_users_mv") end)
    end

    test "schemaless" do
      assert {4, _} =
               TestRepo.insert_all(
                 "agg_test_users_null",
                 [
                   [uid: 1231, updated: ~N[2020-01-02 00:00:00], name: "Jane"],
                   [uid: 1231, updated: ~N[2020-01-01 00:00:00], name: "John"]
                 ],
                 types: [uid: "Int16", updated: :datetime, name: :string]
               )

      assert "agg_test_users"
             |> select([u], %{uid: u.uid, updated: max(u.updated), name: argMaxMerge(u.name)})
             |> group_by([u], u.uid)
             |> TestRepo.all() == [%{uid: 1231, updated: ~N[2020-01-02 00:00:00], name: "Jane"}]
    end

    defmodule UserNull do
      use Ecto.Schema

      @primary_key false
      schema "agg_test_users_null" do
        field :uid, Ch, type: "Int16"
        field :updated, :naive_datetime
        field :name, :string
      end
    end

    test "schemafull" do
      assert {4, _} =
               TestRepo.insert_all(
                 UserNull,
                 [
                   [uid: 1231, updated: ~N[2020-01-02 00:00:00], name: "Jane"],
                   [uid: 1231, updated: ~N[2020-01-01 00:00:00], name: "John"]
                 ]
               )

      assert "agg_test_users"
             |> select([u], %{uid: u.uid, updated: max(u.updated), name: argMaxMerge(u.name)})
             |> group_by([u], u.uid)
             |> TestRepo.all() == [%{uid: 1231, updated: ~N[2020-01-02 00:00:00], name: "Jane"}]
    end
  end
end
