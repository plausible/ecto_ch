defmodule Ecto.Integration.UpdateQueryRegressionTest do
  use Ecto.Integration.Case, async: true

  import Ecto.Query

  alias Ecto.Integration.TestRepo

  @moduletag :update
  @update_opts [settings: [allow_experimental_lightweight_update: 1, mutations_sync: 1]]

  defmodule Row do
    use Ecto.Schema

    @primary_key {:id, Ch, type: "UInt64"}
    schema "update_query_regression_rows" do
      field :num, Ch, type: "UInt8"
      field :labels, Ch, type: "Array(Nullable(String))"
      field :nested_labels, Ch, type: "Array(Array(Nullable(String)))"
    end
  end

  setup do
    TestRepo.query!("""
    CREATE TABLE update_query_regression_rows (
      id UInt64,
      num UInt8,
      labels Array(Nullable(String)),
      nested_labels Array(Array(Nullable(String)))
    ) ENGINE MergeTree ORDER BY id
    SETTINGS enable_block_number_column = true, enable_block_offset_column = true
    """)

    on_exit(fn -> TestRepo.query!("DROP TABLE update_query_regression_rows") end)

    TestRepo.insert_all(Row, [
      %{id: 1, num: 0, labels: ["before"], nested_labels: [["before"]]},
      %{id: 2, num: 0, labels: ["untouched"], nested_labels: [["untouched"]]}
    ])

    :ok
  end

  test "bulk updates preserve leading, trailing and exclusively nil array elements" do
    for operation <- [:update_all, :alter_update_all],
        labels <- [["after", nil], [nil, "after", nil], [nil, nil]] do
      query = from row in Row, where: row.id == 1, update: [set: [labels: ^labels]]

      assert {0, nil} = apply(TestRepo, operation, [query, [], @update_opts])
      assert %Row{labels: ^labels} = TestRepo.get!(Row, 1)
      assert %Row{labels: ["untouched"]} = TestRepo.get!(Row, 2)
    end
  end

  test "bulk updates preserve nullable elements across nested arrays" do
    labels = [[], ["after"], [nil]]

    for operation <- [:update_all, :alter_update_all] do
      query = from row in Row, where: row.id == 1, update: [set: [nested_labels: ^labels]]

      assert {0, nil} = apply(TestRepo, operation, [query, [], @update_opts])
      assert %Row{nested_labels: ^labels} = TestRepo.get!(Row, 1)
      assert %Row{nested_labels: [["untouched"]]} = TestRepo.get!(Row, 2)
    end
  end

  test "bulk updates reject target correlations before changing any rows" do
    matches =
      from other in Row,
        where: other.id == parent_as(:outer).id and other.id == 1,
        select: 1

    query =
      from row in Row,
        as: :outer,
        where: exists(subquery(matches)),
        update: [set: [num: 1]]

    for operation <- [:update_all, :alter_update_all] do
      assert_raise Ecto.QueryError,
                   ~r/correlated subqueries referencing the target table/,
                   fn -> apply(TestRepo, operation, [query, [], @update_opts]) end
    end

    assert TestRepo.all(from row in Row, order_by: row.id, select: {row.id, row.num}) == [
             {1, 0},
             {2, 0}
           ]
  end
end
