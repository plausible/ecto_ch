defmodule Ecto.Integration.UpdateTest do
  use Ecto.Integration.Case, async: true

  import Ecto.Query

  alias Ecto.Integration.TestRepo

  @moduletag :update
  @update_opts [settings: [allow_experimental_lightweight_update: 1]]

  defmodule Profile do
    use Ecto.Schema

    @primary_key {:id, Ch, type: "UInt64"}
    schema "lightweight_update_profiles" do
      field :name, Ch, type: "Nullable(String)"
      field :score, Ch, type: "UInt8"
    end
  end

  defmodule TypedProfile do
    use Ecto.Schema

    @primary_key {:id, Ch, type: "UInt64"}
    schema "lightweight_update_typed_profiles" do
      field :labels, Ch, type: "Array(Nullable(String))", source: :tags
      field :pair, Ch, type: "Tuple(String, Int16)"
      field :point, Ch, type: "Point"
      field :address, Ch, type: "IPv4"
      field :addresses, Ch, type: "Array(IPv6)"
      field :payload, Ch, type: "JSON"
      field :uuid, Ecto.UUID
      field :recorded_at, :naive_datetime_usec
      field :amount, Ch, type: "Decimal(18, 4)"
    end
  end

  test "to_sql/2 generates a lightweight UPDATE" do
    query =
      "events"
      |> where([e], e.name == ^"hello")
      |> update([e], set: [i: ^1], inc: [j: e.j])

    assert TestRepo.to_sql(:update_all, query) == {
             ~s[UPDATE "events" SET "j"="j"+"j","i"={$0:Int64} WHERE ("name" = {$1:String})],
             [1, "hello"]
           }
  end

  test "update_all updates every row when no filter is provided" do
    create_table("lightweight_update_all_rows", "s String, i UInt8")

    TestRepo.query!("""
    INSERT INTO lightweight_update_all_rows VALUES ('Hello', 0), ('World', 0)
    """)

    assert {0, nil} =
             TestRepo.update_all(
               "lightweight_update_all_rows",
               [set: [i: 10]],
               @update_opts
             )

    assert rows("lightweight_update_all_rows", [:s, :i]) == [
             %{s: "Hello", i: 10},
             %{s: "World", i: 10}
           ]
  end

  test "update_all supports set and increment operations" do
    create_table("lightweight_update_numbers", "s String, i Int16")

    TestRepo.query!("""
    INSERT INTO lightweight_update_numbers VALUES ('Hello', 0), ('World', 0)
    """)

    "lightweight_update_numbers"
    |> where([row], row.s == "Hello")
    |> update([row], set: [i: 1])
    |> TestRepo.update_all([], @update_opts)

    "lightweight_update_numbers"
    |> where([row], row.s == ^"World")
    |> TestRepo.update_all([set: [i: 2]], @update_opts)

    "lightweight_update_numbers"
    |> where([row], row.s == "Hello")
    |> TestRepo.update_all([inc: [i: 2]], @update_opts)

    "lightweight_update_numbers"
    |> where([row], row.s == ^"World")
    |> update([row], inc: [i: ^(-1)])
    |> TestRepo.update_all([], @update_opts)

    assert rows("lightweight_update_numbers", [:s, :i]) == [
             %{s: "Hello", i: 3},
             %{s: "World", i: 1}
           ]
  end

  test "update_all supports push and pull operations" do
    create_table("lightweight_update_arrays", "s String, arr Array(UInt8)")

    TestRepo.query!("""
    INSERT INTO lightweight_update_arrays
    VALUES ('Hello', [1, 2]), ('World', [3, 4, 5]), ('Goodbye', [])
    """)

    "lightweight_update_arrays"
    |> where([row], row.s == "Goodbye")
    |> TestRepo.update_all([push: [arr: 6]], @update_opts)

    "lightweight_update_arrays"
    |> where([row], row.s == ^"World")
    |> update([row], push: [arr: ^7])
    |> TestRepo.update_all([], @update_opts)

    "lightweight_update_arrays"
    |> where([row], row.s == ^"World")
    |> update([row], pull: [arr: ^4])
    |> TestRepo.update_all([], @update_opts)

    "lightweight_update_arrays"
    |> where([row], row.s == "Hello")
    |> TestRepo.update_all([pull: [arr: 1]], @update_opts)

    assert rows("lightweight_update_arrays", [:s, :arr]) == [
             %{s: "Goodbye", arr: [6]},
             %{s: "Hello", arr: [2]},
             %{s: "World", arr: [3, 5, 7]}
           ]
  end

  test "Repo.update updates a schema and supports nil values" do
    create_table(
      "lightweight_update_profiles",
      "id UInt64, name Nullable(String), score UInt8",
      "id"
    )

    profile = TestRepo.insert!(%Profile{id: 1, name: "before", score: 1})
    changeset = Ecto.Changeset.change(profile, name: nil, score: 2)

    assert {:ok, %Profile{name: nil, score: 2}} = TestRepo.update(changeset, @update_opts)
    assert %Profile{name: nil, score: 2} = TestRepo.get!(Profile, profile.id)
  end

  test "update_all supports schema field expressions and subquery filters" do
    create_table(
      "lightweight_update_profiles",
      "id UInt64, name Nullable(String), score UInt8",
      "id"
    )

    TestRepo.insert_all(Profile, [
      %{id: 1, name: "before", score: 2},
      %{id: 2, name: "unchanged", score: 4}
    ])

    ids = from(p in Profile, where: p.score < ^3, select: p.id)

    query =
      from p in Profile,
        where: p.id in subquery(ids),
        update: [set: [name: fragment("concat(?, ?)", p.name, ^" after")], inc: [score: p.score]]

    assert {0, nil} = TestRepo.update_all(query, [], @update_opts)
    assert %Profile{name: "before after", score: 4} = TestRepo.get!(Profile, 1)
    assert %Profile{name: "unchanged", score: 4} = TestRepo.get!(Profile, 2)
  end

  test "Repo.update handles nil filters without shifting subsequent parameters" do
    create_table(
      "lightweight_update_profiles",
      "id UInt64, name Nullable(String), score UInt8",
      "id"
    )

    profile = TestRepo.insert!(%Profile{id: 1, name: nil, score: 1})
    changeset = Ecto.Changeset.change(profile, score: 2)
    changeset = %{changeset | filters: %{name: nil}}

    assert {:ok, %Profile{score: 2}} = TestRepo.update(changeset, @update_opts)
    assert %Profile{name: nil, score: 2} = TestRepo.get!(Profile, profile.id)
  end

  test "Repo.update preserves schema types and field sources" do
    create_table(
      "lightweight_update_typed_profiles",
      """
      id UInt64,
      tags Array(Nullable(String)),
      pair Tuple(String, Int16),
      point Point,
      address IPv4,
      addresses Array(IPv6),
      payload JSON,
      uuid UUID,
      recorded_at DateTime64(6),
      amount Decimal(18, 4)
      """,
      "id"
    )

    profile =
      TestRepo.insert!(%TypedProfile{
        id: 1,
        labels: ["before"],
        pair: {"before", 1},
        point: {1.0, 2.0},
        address: {127, 0, 0, 1},
        addresses: [{0, 0, 0, 0, 0, 0, 0, 1}],
        payload: %{name: "before"},
        uuid: Ecto.UUID.generate(),
        recorded_at: ~N[2026-01-01 00:00:00.000000],
        amount: Decimal.new("1.0000")
      })

    changes = [
      labels: [nil, "after", nil],
      pair: {"after", 2},
      point: {3.0, 4.0},
      address: {127, 0, 0, 2},
      addresses: [{0, 0, 0, 0, 0, 0, 0, 2}],
      payload: %{name: "after", count: 2},
      uuid: Ecto.UUID.generate(),
      recorded_at: ~N[2026-01-02 03:04:05.123456],
      amount: Decimal.new("2.3456")
    ]

    assert {:ok, %TypedProfile{}} =
             profile |> Ecto.Changeset.change(changes) |> TestRepo.update(@update_opts)

    updated = TestRepo.get!(TypedProfile, profile.id)

    for {field, value} <- Keyword.delete(changes, :payload) do
      assert Map.fetch!(updated, field) == value
    end

    assert updated.payload == %{"name" => "after", "count" => 2}
  end

  defp create_table(name, columns, order_by \\ "tuple()") do
    TestRepo.query!("""
    CREATE TABLE #{name} (#{columns})
    ENGINE MergeTree
    ORDER BY #{order_by}
    SETTINGS
      enable_block_number_column = true,
      enable_block_offset_column = true
    """)

    on_exit(fn -> TestRepo.query!("DROP TABLE #{name}") end)
  end

  defp rows(table, fields) do
    table
    |> select([row], map(row, ^fields))
    |> order_by([row], asc: row.s)
    |> TestRepo.all()
  end
end
