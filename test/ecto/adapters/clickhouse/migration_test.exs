defmodule Ecto.Adapters.ClickHouse.MigrationTest do
  use ExUnit.Case

  alias Ecto.Adapters.ClickHouse

  defmodule MigrationRepo do
    use Ecto.Repo, adapter: Ecto.Adapters.ClickHouse, otp_app: :migration_test
  end

  defmodule Table do
    use Ecto.Migration

    def change do
      create table("events",
               primary_key: false,
               engine: "MergeTree",
               options:
                 "PARTITION BY toYYYYMM(timestamp) ORDER BY (domain, toDate(timestamp), user_id) SETTINGS index_granularity = 8192"
             ) do
        add :name, :string
        add :domain, :string
        add :user_id, :UInt64
        add :session_id, :UInt64
        add :hostname, :string
        add :pathname, :string
        add :referrer, :string
        add :referrer_source, :string
        add :country_code, :"LowCardinality(FixedString(2))"
        add :screen_size, :"LowCardinality(String)"
        add :operating_system, :"LowCardinality(String)"
        add :browser, :"LowCardinality(String)"
        add :timestamp, :naive_datetime
      end
    end
  end

  defmodule Index do
    use Ecto.Migration

    def change do
      create index(:events, [:name], options: [type: :bloom_filter, granularity: 8192])
    end
  end

  defmodule DropIndex do
    use Ecto.Migration

    def change do
      drop index(:events, [:name])
    end
  end

  test "events (table+index)" do
    database = "ecto_ch_migration_test_events"
    opts = [database: database]

    assert :ok = ClickHouse.storage_up(opts)
    on_exit(fn -> ClickHouse.storage_down(opts) end)

    Application.put_env(:migration_test, MigrationRepo,
      database: database,
      show_sensitive_data_on_connection_error: true
    )

    on_exit(fn -> Application.delete_env(:migration_test, MigrationRepo) end)

    start_supervised!(MigrationRepo)

    assert [1, 2] ==
             Ecto.Migrator.run(MigrationRepo, [{1, Table}, {2, Index}], :up,
               all: true,
               log: false
             )

    conn = start_supervised!({Ch, opts})

    assert Ch.query!(
             conn,
             "select create_table_query from system.tables where database = {database:String} and table = {table:String}",
             %{"database" => database, "table" => "events"}
           ).rows == [
             [
               """
               CREATE TABLE ecto_ch_migration_test_events.events (\
               `name` String, \
               `domain` String, \
               `user_id` UInt64, \
               `session_id` UInt64, \
               `hostname` String, \
               `pathname` String, \
               `referrer` String, \
               `referrer_source` String, \
               `country_code` LowCardinality(FixedString(2)), \
               `screen_size` LowCardinality(String), \
               `operating_system` LowCardinality(String), \
               `browser` LowCardinality(String), \
               `timestamp` DateTime, \
               INDEX events_name_index name TYPE bloom_filter GRANULARITY 8192\
               ) \
               ENGINE = MergeTree \
               PARTITION BY toYYYYMM(timestamp) \
               ORDER BY (domain, toDate(timestamp), user_id) \
               SETTINGS index_granularity = 8192\
               """
             ]
           ]

    assert [3] ==
             Ecto.Migrator.run(MigrationRepo, [{3, DropIndex}], :up,
               all: true,
               log: false
             )

    assert Ch.query!(
             conn,
             "select create_table_query from system.tables where database = {database:String} and table = {table:String}",
             %{"database" => database, "table" => "events"}
           ).rows == [
             [
               """
               CREATE TABLE ecto_ch_migration_test_events.events (\
               `name` String, \
               `domain` String, \
               `user_id` UInt64, \
               `session_id` UInt64, \
               `hostname` String, \
               `pathname` String, \
               `referrer` String, \
               `referrer_source` String, \
               `country_code` LowCardinality(FixedString(2)), \
               `screen_size` LowCardinality(String), \
               `operating_system` LowCardinality(String), \
               `browser` LowCardinality(String), \
               `timestamp` DateTime\
               ) \
               ENGINE = MergeTree \
               PARTITION BY toYYYYMM(timestamp) \
               ORDER BY (domain, toDate(timestamp), user_id) \
               SETTINGS index_granularity = 8192\
               """
             ]
           ]
  end
end
