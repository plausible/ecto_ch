defmodule Ecto.Integration.DeleteAllTest do
  use Ecto.Integration.Case
  import Ecto.Query
  alias Ecto.Integration.TestRepo

  @moduletag :lightweight_delete

  # based on https://github.com/plausible/ecto_ch/issues/247

  setup do
    TestRepo.query!("""
    CREATE TABLE user_id_map (
      account_id UInt64,
      user_id_hash UInt64,
      user_id String
    ) ENGINE ReplacingMergeTree() ORDER BY (account_id, user_id_hash)
    """)

    on_exit(fn -> TestRepo.query!("DROP TABLE user_id_map") end)

    TestRepo.query!("""
    CREATE TABLE recent_user_profiles (
      account_id UInt64,
      user_id_hash UInt64,
      timestamp DateTime64(3)
    ) ENGINE ReplacingMergeTree(timestamp)
    ORDER BY (account_id, user_id_hash)
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1
    """)

    on_exit(fn -> TestRepo.query!("DROP TABLE recent_user_profiles") end)

    :ok
  end

  defmodule UserIdMap do
    use Ecto.Schema

    @primary_key false
    schema "user_id_map" do
      field :account_id, Ch, type: "UInt64"
      field :user_id_hash, Ch, type: "UInt64"
      field :user_id, :string
    end
  end

  defmodule RecentUserProfiles do
    use Ecto.Schema

    @primary_key false
    schema "recent_user_profiles" do
      field :account_id, Ch, type: "UInt64"
      field :user_id_hash, Ch, type: "UInt64"
      field :timestamp, Ch, type: "DateTime64(3)"
    end
  end

  test "delete_all with subqeury" do
    TestRepo.insert!(%UserIdMap{account_id: 91241, user_id_hash: 100, user_id: "anon:123"})
    TestRepo.insert!(%UserIdMap{account_id: 91241, user_id_hash: 200, user_id: "registered:bob"})

    TestRepo.insert!(%RecentUserProfiles{
      account_id: 91241,
      user_id_hash: 100,
      timestamp: ~N[2023-01-01 10:00:00.000000]
    })

    TestRepo.insert!(%RecentUserProfiles{
      account_id: 91241,
      user_id_hash: 200,
      timestamp: ~N[2023-01-01 12:00:00.000000]
    })

    list_recent_user_profiles = fn ->
      TestRepo.all(
        from rup in RecentUserProfiles,
          inner_join: uim in UserIdMap,
          on: uim.user_id_hash == rup.user_id_hash,
          order_by: [asc: :timestamp],
          select: uim.user_id
      )
    end

    assert list_recent_user_profiles.() == ["anon:123", "registered:bob"]

    hashes =
      from uim in UserIdMap,
        where: uim.account_id == ^91241 and like(uim.user_id, "anon:%"),
        select: uim.user_id_hash

    query =
      from rup in RecentUserProfiles,
        where: rup.account_id == ^91241 and rup.user_id_hash in subquery(hashes)

    TestRepo.delete_all(query,
      settings: [
        lightweight_deletes_sync: 0,
        lightweight_delete_mode: "lightweight_update_force",
        alter_update_mode: "lightweight"
      ]
    )

    assert list_recent_user_profiles.() == ["registered:bob"]
  end
end
