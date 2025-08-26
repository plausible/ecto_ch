defmodule Ecto.Integration.SchemaTest do
  use Ecto.Integration.Case
  import Ecto.Query, only: [from: 2]
  alias Ecto.Integration.TestRepo

  setup do
    TestRepo.query!("""
    create table schema_test (
      id UInt64,
      array_tuple_dynamic Array(Tuple(a LowCardinality(String), b LowCardinality(String), c LowCardinality(String), d Dynamic))
    ) ENGINE = MergeTree ORDER BY tuple()
    """)

    on_exit(fn -> TestRepo.query!("drop table schema_test") end)
  end

  defmodule Schema do
    use Ecto.Schema

    @primary_key false
    schema "schema_test" do
      field :id, Ch, type: "UInt64"

      # TODO preserve names in named tuples
      field :array_tuple_dynamic, {:array, Ch},
        type:
          "Tuple(LowCardinality(String), LowCardinality(String), LowCardinality(String), Dynamic)"
    end
  end

  describe "insert_all/3" do
    @tag :dynamic
    test "with array of tuples containing dynamic type" do
      TestRepo.insert_all(
        Schema,
        [
          %{id: 1, array_tuple_dynamic: [{"a1", "b1", "c1", "d1"}]},
          %{id: 2, array_tuple_dynamic: []}
        ],
        # TODO remove this workaround when Ch preserves names in named tuples
        settings: [input_format_with_types_use_header: 0]
      )

      assert TestRepo.all(
               from s in Schema,
                 select: map(s, [:id, :array_tuple_dynamic]),
                 order_by: s.id
             ) == [
               %{id: 1, array_tuple_dynamic: [{"a1", "b1", "c1", "d1"}]},
               %{id: 2, array_tuple_dynamic: []}
             ]
    end
  end
end
