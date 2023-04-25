defmodule Ecto.Adapters.ClickHouse.StructureTest do
  use ExUnit.Case

  alias Ecto.Adapters.ClickHouse

  defmodule Repo do
    use Ecto.Repo, adapter: Ecto.Adapters.ClickHouse, otp_app: :structure_test
  end

  defmodule Migration1 do
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

      create table("sessions",
               primary_key: false,
               engine: "CollapsingMergeTree(sign)",
               options:
                 "PARTITION BY toYYYYMM(start) ORDER BY (domain, toDate(start), user_id, session_id) SETTINGS index_granularity = 8192"
             ) do
        add :session_id, :UInt64
        add :sign, :Int8
        add :domain, :string
        add :user_id, :UInt64
        add :hostname, :string
        add :is_bounce, :boolean
        add :entry_page, :string
        add :exit_page, :string
        add :pageviews, :integer
        add :events, :integer
        add :duration, :UInt32
        add :referrer, :string
        add :referrer_source, :string
        add :country_code, :"LowCardinality(FixedString(2))"
        add :screen_size, :"LowCardinality(String)"
        add :operating_system, :"LowCardinality(String)"
        add :browser, :"LowCardinality(String)"

        add :start, :naive_datetime
        add :timestamp, :naive_datetime
      end
    end
  end

  defmodule Migration2 do
    use Ecto.Migration

    def change do
      create table(:ingest_counters,
               primary_key: false,
               engine: "SummingMergeTree(value)",
               options:
                 "ORDER BY (domain, toDate(event_timebucket), metric, toStartOfMinute(event_timebucket))"
             ) do
        add :event_timebucket, :utc_datetime
        add :domain, :"LowCardinality(String)"
        add :site_id, :"Nullable(UInt64)"
        add :metric, :"LowCardinality(String)"
        add :value, :UInt64
      end
    end
  end

  describe "structure_dump/1" do
    test "dump unknown db" do
      opts = [database: "ecto_ch_does_not_exist"]

      assert {:error, %Ch.Error{code: 81, message: message}} =
               ClickHouse.structure_dump("priv/repo", opts)

      assert message =~ "UNKNOWN_DATABASE"
    end

    test "dumps empty database" do
      opts = [database: "ecto_ch_temp_structure_empty"]

      assert :ok = ClickHouse.storage_up(opts)
      on_exit(fn -> ClickHouse.storage_down(opts) end)

      assert {:error, %Ch.Error{code: 390, message: message}} =
               ClickHouse.structure_dump("priv/repo", opts)

      assert message =~ "CANNOT_GET_CREATE_TABLE_QUERY"
    end

    test "dumps migrated database" do
      database = "ecto_ch_temp_structure_migrated"
      opts = [database: database]

      assert :ok = ClickHouse.storage_up(opts)
      on_exit(fn -> ClickHouse.storage_down(opts) end)

      Application.put_env(:structure_test, Repo,
        database: database,
        show_sensitive_data_on_connection_error: true
      )

      on_exit(fn -> Application.delete_env(:structure_test, Repo) end)

      start_supervised!(Repo)

      assert [1, 2] ==
               Ecto.Migrator.run(Repo, [{1, Migration1}, {2, Migration2}], :up,
                 all: true,
                 log: false
               )

      tmp = System.tmp_dir!()

      assert {:ok, path} = ClickHouse.structure_dump(tmp, opts)
      on_exit(fn -> File.rm!(path) end)

      structure = File.read!(path)
      parts = String.split(structure, "\n\n")

      find_schema = fn name ->
        Enum.find(parts, fn part ->
          String.starts_with?(part, "CREATE TABLE " <> database <> "." <> name)
        end)
      end

      assert find_schema.("events") == """
             CREATE TABLE ecto_ch_temp_structure_migrated.events
             (
                 `name` String,
                 `domain` String,
                 `user_id` UInt64,
                 `session_id` UInt64,
                 `hostname` String,
                 `pathname` String,
                 `referrer` String,
                 `referrer_source` String,
                 `country_code` LowCardinality(FixedString(2)),
                 `screen_size` LowCardinality(String),
                 `operating_system` LowCardinality(String),
                 `browser` LowCardinality(String),
                 `timestamp` DateTime
             )
             ENGINE = MergeTree
             PARTITION BY toYYYYMM(timestamp)
             ORDER BY (domain, toDate(timestamp), user_id)
             SETTINGS index_granularity = 8192;\
             """

      assert find_schema.("sessions") == """
             CREATE TABLE ecto_ch_temp_structure_migrated.sessions
             (
                 `session_id` UInt64,
                 `sign` Int8,
                 `domain` String,
                 `user_id` UInt64,
                 `hostname` String,
                 `is_bounce` Bool,
                 `entry_page` String,
                 `exit_page` String,
                 `pageviews` Int32,
                 `events` Int32,
                 `duration` UInt32,
                 `referrer` String,
                 `referrer_source` String,
                 `country_code` LowCardinality(FixedString(2)),
                 `screen_size` LowCardinality(String),
                 `operating_system` LowCardinality(String),
                 `browser` LowCardinality(String),
                 `start` DateTime,
                 `timestamp` DateTime
             )
             ENGINE = CollapsingMergeTree(sign)
             PARTITION BY toYYYYMM(start)
             ORDER BY (domain, toDate(start), user_id, session_id)
             SETTINGS index_granularity = 8192;\
             """

      assert find_schema.("ingest_counters") == """
             CREATE TABLE ecto_ch_temp_structure_migrated.ingest_counters
             (
                 `event_timebucket` DateTime,
                 `domain` LowCardinality(String),
                 `site_id` Nullable(UInt64),
                 `metric` LowCardinality(String),
                 `value` UInt64
             )
             ENGINE = SummingMergeTree(value)
             ORDER BY (domain, toDate(event_timebucket), metric, toStartOfMinute(event_timebucket))
             SETTINGS index_granularity = 8192;\
             """

      schema_migrations = find_schema.("schema_migrations")

      assert schema_migrations == """
             CREATE TABLE ecto_ch_temp_structure_migrated.schema_migrations
             (
                 `version` Int64,
                 `inserted_at` DateTime
             )
             ENGINE = TinyLog;\
             """

      assert Enum.at(parts, -2) == schema_migrations

      conn = start_supervised!({Ch, opts})

      %{rows: [[1, inserted_at_1], [2, inserted_at_2]]} =
        Ch.query!(conn, "select * from schema_migrations order by version")

      assert List.last(parts) ==
               """
               INSERT INTO "ecto_ch_temp_structure_migrated"."schema_migrations" (version, inserted_at) VALUES
               (1,'#{inserted_at_1}'),
               (2,'#{inserted_at_2}');
               """
    end
  end
end
