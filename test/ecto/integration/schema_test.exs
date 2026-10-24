defmodule Ecto.Integration.SchemaTest do
  use Ecto.Integration.Case
  import Ecto.Query, only: [from: 2]
  alias Ecto.Integration.TestRepo

  @moduletag :dynamic

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

      field :array_tuple_dynamic, {:array, Ch},
        type:
          "Tuple(a LowCardinality(String), b LowCardinality(String), c LowCardinality(String), d Dynamic)"
    end
  end

  describe "insert_all/3" do
    test "with array of tuples containing dynamic type" do
      assert {2, nil} =
               TestRepo.insert_all(
                 Schema,
                 [
                   %{id: 1, array_tuple_dynamic: [{"a1", "b1", "c1", "d1"}]},
                   %{id: 2, array_tuple_dynamic: []}
                 ],
                 # Ch discards tuple element names when initializing schema types.
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

  test "insert!/2 with array of tuples containing dynamic type" do
    values = [{"a1", "b1", "c1", "d1"}, {"a2", "b2", "c2", 42}]

    TestRepo.insert!(%Schema{id: 1, array_tuple_dynamic: values},
      settings: [input_format_with_types_use_header: 0]
    )

    assert TestRepo.one!(from s in Schema, select: s.array_tuple_dynamic) == values
  end

  test "insert_stream/3 with array of tuples containing dynamic type" do
    rows = [
      %{id: 1, array_tuple_dynamic: [{"a1", "b1", "c1", "d1"}]},
      %{id: 2, array_tuple_dynamic: []}
    ]

    assert {2, nil} =
             TestRepo.insert_stream(Schema, Stream.map(rows, & &1),
               chunk_every: 1,
               settings: [input_format_with_types_use_header: 0]
             )

    assert TestRepo.all(
             from s in Schema, select: map(s, [:id, :array_tuple_dynamic]), order_by: s.id
           ) == rows
  end

  test "schemaless insert_stream/3 preserves named tuple types containing dynamic type" do
    rows = [
      %{id: 1, array_tuple_dynamic: [{"a1", "b1", "c1", "d1"}, {"a2", "b2", "c2", 42}]},
      %{id: 2, array_tuple_dynamic: []}
    ]

    assert {2, nil} =
             TestRepo.insert_stream("schema_test", Stream.map(rows, & &1),
               chunk_every: 1,
               types: [
                 id: "UInt64",
                 array_tuple_dynamic:
                   "Array(Tuple(a LowCardinality(String), b LowCardinality(String), c LowCardinality(String), d Dynamic))"
               ]
             )

    assert TestRepo.all(
             from s in Schema, select: map(s, [:id, :array_tuple_dynamic]), order_by: s.id
           ) == rows
  end
end
