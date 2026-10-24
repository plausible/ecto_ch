defmodule Ecto.Integration.UpdateParameterRegressionTest do
  use Ecto.Integration.Case, async: true

  alias Ecto.Integration.TestRepo

  @moduletag :update
  @update_opts [settings: [allow_experimental_lightweight_update: 1]]

  defmodule DynamicValues do
    use Ecto.Schema

    @primary_key {:id, Ch, type: "UInt64"}
    schema "lightweight_update_nested_dynamic" do
      field :values, Ch, type: "Array(Dynamic)"
      field :pair, Ch, type: "Tuple(Dynamic, Dynamic)"
      field :mapping, Ch, type: "Map(String, Dynamic)"
    end
  end

  defmodule VariantValues do
    use Ecto.Schema

    @primary_key {:id, Ch, type: "UInt64"}
    schema "lightweight_update_nested_variant" do
      field :addresses, Ch, type: "Array(Variant(IPv4, String))"
      field :pair, Ch, type: "Tuple(Variant(IPv4, String), Variant(Date, String))"
      field :mapping, Ch, type: "Map(String, Variant(IPv4, String))"
    end
  end

  defmodule ContainerVariants do
    use Ecto.Schema

    @primary_key {:id, Ch, type: "UInt64"}
    schema "lightweight_update_container_variants" do
      field :values, Ch, type: "Array(Variant(Array(Nullable(String)), String))"
    end
  end

  defmodule TimeValues do
    use Ecto.Schema

    @primary_key {:id, Ch, type: "UInt64"}
    schema "lightweight_update_nested_time" do
      field :times, Ch, type: "Array(Time)"
      field :precise_times, Ch, type: "Array(Time64(6))"
      field :pair, Ch, type: "Tuple(Time, Time64(6))"
      field :mapping, Ch, type: "Map(String, Time)"
    end
  end

  @tag :dynamic
  test "Repo.update preserves nested Dynamic values without inferring strings as dates" do
    create_table(
      "lightweight_update_nested_dynamic",
      "id UInt64, values Array(Dynamic), pair Tuple(Dynamic, Dynamic), mapping Map(String, Dynamic)"
    )

    original =
      TestRepo.insert!(%DynamicValues{
        id: 1,
        values: ["before", 1],
        pair: {"before", 1},
        mapping: %{"value" => "before"}
      })

    changes = [
      values: ["2026-01-01", "123", 2, ~D[2026-01-02]],
      pair: {"2026-01-03", ~D[2026-01-04]},
      mapping: %{"date_string" => "2026-01-05", "date" => ~D[2026-01-06], "count" => 3}
    ]

    assert {:ok, %DynamicValues{}} =
             original |> Ecto.Changeset.change(changes) |> TestRepo.update(@update_opts)

    updated = TestRepo.get!(DynamicValues, original.id)

    for {field, value} <- changes do
      assert {field, Map.fetch!(updated, field)} == {field, value}
    end

    assert {:ok, %DynamicValues{}} =
             updated
             |> Ecto.Changeset.change(values: [], pair: {nil, []}, mapping: %{})
             |> TestRepo.update(@update_opts)

    assert %DynamicValues{values: [], pair: {nil, []}, mapping: %{}} =
             TestRepo.get!(DynamicValues, original.id)
  end

  @tag :variant
  test "Repo.update preserves heterogeneous nested Variant members and IP addresses" do
    create_table(
      "lightweight_update_nested_variant",
      """
      id UInt64,
      addresses Array(Variant(IPv4, String)),
      pair Tuple(Variant(IPv4, String), Variant(Date, String)),
      mapping Map(String, Variant(IPv4, String))
      """
    )

    original =
      TestRepo.insert!(%VariantValues{
        id: 1,
        addresses: [{127, 0, 0, 1}, "before"],
        pair: {{127, 0, 0, 1}, "before"},
        mapping: %{"address" => {127, 0, 0, 1}}
      })

    changes = [
      addresses: [{127, 0, 0, 2}, "127.0.0.3", "2026-01-01", nil],
      pair: {{127, 0, 0, 4}, "2026-01-02"},
      mapping: %{"address" => {127, 0, 0, 5}, "string" => "127.0.0.6", "null" => nil}
    ]

    assert {:ok, %VariantValues{}} =
             original |> Ecto.Changeset.change(changes) |> TestRepo.update(@update_opts)

    updated = TestRepo.get!(VariantValues, original.id)

    for {field, value} <- changes do
      assert {field, Map.fetch!(updated, field)} == {field, value}
    end
  end

  @tag :variant
  test "Repo.update preserves nested Variant container members and nullable elements" do
    create_table(
      "lightweight_update_container_variants",
      "id UInt64, values Array(Variant(Array(Nullable(String)), String))"
    )

    original = TestRepo.insert!(%ContainerVariants{id: 1, values: [["before"], "before"]})
    values = [[nil, "2026-01-01", nil], "2026-01-02", [], nil]

    assert {:ok, %ContainerVariants{}} =
             original |> Ecto.Changeset.change(values: values) |> TestRepo.update(@update_opts)

    assert %ContainerVariants{values: ^values} = TestRepo.get!(ContainerVariants, original.id)
  end

  @tag :time
  test "Repo.update quotes Time and Time64 values inside composite parameters" do
    create_table(
      "lightweight_update_nested_time",
      """
      id UInt64,
      times Array(Time),
      precise_times Array(Time64(6)),
      pair Tuple(Time, Time64(6)),
      mapping Map(String, Time)
      """
    )

    original =
      TestRepo.insert!(%TimeValues{
        id: 1,
        times: [~T[01:02:03]],
        precise_times: [~T[01:02:03.123456]],
        pair: {~T[01:02:03], ~T[01:02:03.123456]},
        mapping: %{"time" => ~T[01:02:03]}
      })

    changes = [
      times: [~T[02:03:04], ~T[23:59:59]],
      precise_times: [~T[02:03:04.654321], ~T[23:59:59.000001]],
      pair: {~T[02:03:04], ~T[02:03:04.654321]},
      mapping: %{"time" => ~T[02:03:04]}
    ]

    assert {:ok, %TimeValues{}} =
             original |> Ecto.Changeset.change(changes) |> TestRepo.update(@update_opts)

    updated = TestRepo.get!(TimeValues, original.id)

    for {field, value} <- changes do
      assert {field, Map.fetch!(updated, field)} == {field, value}
    end
  end

  defp create_table(name, columns) do
    TestRepo.query!("""
    CREATE TABLE #{name} (#{columns})
    ENGINE MergeTree
    ORDER BY id
    SETTINGS
      enable_block_number_column = true,
      enable_block_offset_column = true
    """)

    on_exit(fn -> TestRepo.query!("DROP TABLE #{name}") end)
  end
end
