defmodule Ecto.Adapters.ClickHouse.ExplainTest do
  use ExUnit.Case

  import Ecto.Query
  alias Ecto.Integration.TestRepo

  @event [:ecto, :integration, :test_repo, :query]

  setup do
    ref = :telemetry_test.attach_event_handlers(self(), [@event])
    on_exit(fn -> :telemetry.detach(ref) end)
    {:ok, telemetry_ref: ref}
  end

  for {type, clause} <- [
        ast: "AST",
        syntax: "SYNTAX",
        query_tree: "QUERY TREE",
        plan: "PLAN",
        pipeline: "PIPELINE"
      ] do
    test "explains parameterized queries as #{type}", %{telemetry_ref: ref} do
      query =
        from number in fragment("numbers(3)"),
          where: number.number > ^1,
          select: number.number

      # Ecto SQL's default transaction wrapper is unavailable on ClickHouse.
      assert {:ok, rows} =
               Ecto.Adapters.SQL.explain(TestRepo, :all, query,
                 type: unquote(type),
                 wrap_in_transaction: false
               )

      assert rows != []
      assert Enum.all?(rows, fn [line] -> is_binary(line) end)
      assert Enum.any?(rows, fn [line] -> line != "" end)

      assert_received {@event, ^ref, _measurements, %{query: sql, params: [1]}}
      assert to_string(sql) =~ "EXPLAIN #{unquote(clause)} SELECT"
      assert to_string(sql) =~ "{$0:Int64}"
    end
  end

  test "uses the plan format by default", %{telemetry_ref: ref} do
    query = from number in fragment("numbers(1)"), select: number.number

    assert {:ok, [_ | _]} =
             Ecto.Adapters.SQL.explain(TestRepo, :all, query, wrap_in_transaction: false)

    assert_received {@event, ^ref, _measurements, %{query: sql, params: []}}
    assert to_string(sql) =~ "EXPLAIN PLAN SELECT"
  end

  test "returns database errors and leaves the connection usable" do
    query = from row in "explain_missing_table", select: row.id

    assert {:error, %Ch.Error{message: message}} =
             Ecto.Adapters.SQL.explain(TestRepo, :all, query, wrap_in_transaction: false)

    assert message =~ "UNKNOWN_TABLE"

    query = from number in fragment("numbers(1)"), select: number.number

    assert {:ok, [_ | _]} =
             Ecto.Adapters.SQL.explain(TestRepo, :all, query, wrap_in_transaction: false)
  end

  test "forwards DELETE plans to ClickHouse without changing stored rows", %{telemetry_ref: ref} do
    TestRepo.query!("CREATE TABLE explain_rows (id UInt8) ENGINE Memory")
    on_exit(fn -> TestRepo.query!("DROP TABLE explain_rows") end)
    TestRepo.query!("INSERT INTO explain_rows VALUES (1)")
    query = from row in "explain_rows", where: row.id == ^1

    assert {:error, %Ch.Error{}} =
             Ecto.Adapters.SQL.explain(TestRepo, :delete_all, query, wrap_in_transaction: false)

    assert_received {@event, ^ref, _measurements, %{query: sql, params: [1]}}
    assert to_string(sql) =~ "EXPLAIN PLAN DELETE FROM"
    assert TestRepo.query!("SELECT id FROM explain_rows").rows == [[1]]
  end

  for {operation, error} <- [
        delete_all: "RETURNING on DELETE statements",
        update_all: "UPDATE statements"
      ] do
    test "preserves existing #{operation} validation for queries with select clauses", %{
      telemetry_ref: ref
    } do
      query = from row in "explain_missing_table", select: row.id

      query =
        if unquote(operation) == :update_all do
          update(query, set: [value: 42])
        else
          query
        end

      assert_raise Ecto.QueryError, ~r/#{unquote(error)}/, fn ->
        Ecto.Adapters.SQL.explain(TestRepo, unquote(operation), query, wrap_in_transaction: false)
      end

      refute_received {@event, ^ref, _, _}
    end
  end
end
