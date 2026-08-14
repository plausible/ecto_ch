defmodule Ecto.Adapters.ClickHouse.MigrationTest do
  use ExUnit.Case

  alias Ecto.Adapters.ClickHouse
  alias Ecto.Adapters.ClickHouse.Connection

  defmodule MigrationRepo do
    use Ecto.Repo, adapter: Ecto.Adapters.ClickHouse, otp_app: :migration_test
  end

  defmodule Table do
    use Ecto.Migration

    def change do
      create table("events",
               primary_key: false,
               engine: "MergeTree",
               options: [
                 partition_by: "toYYYYMM(timestamp)",
                 order_by: "(domain, toDate(timestamp), user_id)",
                 settings: "index_granularity = 8192"
               ]
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

  defmodule CreateProducts do
    use Ecto.Migration

    def change do
      create table(:products, primary_key: false, engine: "Memory") do
        add :name, :string
        add :price, :UInt64
      end
    end
  end

  defmodule AddPriceDefault do
    use Ecto.Migration

    def change do
      alter table(:products) do
        modify :price, :UInt64, default: 1
      end
    end
  end

  defmodule QuotedDDL do
    use Ecto.Migration

    @table_name "ecto_ch quoted ' \" ` \\ ; -- /* */ $tag$ table \\"
    @column_name String.to_atom("value ' \" ` \\ ; -- /* */ $tag$ column \\")
    @escaped_text "single ' double \" backtick ` backslash \\ pair \\' ; -- /* */ $tag$ body $tag$\ntrailing \\"

    def change do
      create table(@table_name,
               primary_key: false,
               engine: "Memory",
               comment: @escaped_text
             ) do
        add :id, :UInt8
        add @column_name, :string, default: @escaped_text, comment: @escaped_text
      end
    end

    def table_name, do: @table_name
    def column_name, do: Atom.to_string(@column_name)
    def escaped_text, do: @escaped_text
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

    [[create_table_query]] =
      Ch.query!(
        conn,
        "select create_table_query from system.tables where database = {database:String} and table = {table:String}",
        %{"database" => database, "table" => "events"}
      ).rows

    # ClickHouse 24.5 omits parentheses around single-column index expressions.
    # Remove this normalization when 24.5 is dropped from the CI matrix.
    create_table_query =
      String.replace(
        create_table_query,
        "INDEX events_name_index name TYPE",
        "INDEX events_name_index (name) TYPE"
      )

    assert create_table_query ==
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
             INDEX events_name_index (name) TYPE bloom_filter GRANULARITY 8192\
             ) \
             ENGINE = MergeTree \
             PARTITION BY toYYYYMM(timestamp) \
             ORDER BY (domain, toDate(timestamp), user_id) \
             SETTINGS index_granularity = 8192\
             """

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

  test "modify column default" do
    database = "ecto_ch_migration_test_modify_default"
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
             Ecto.Migrator.run(MigrationRepo, [{1, CreateProducts}, {2, AddPriceDefault}], :up,
               all: true,
               log: false
             )

    conn = start_supervised!({Ch, opts})

    assert %{num_rows: 1} =
             Ch.query!(conn, "INSERT INTO products (name) VALUES ('book')")

    assert [[1]] == Ch.query!(conn, "SELECT price FROM products").rows
  end

  test "quoted names, defaults, and comments round-trip through ClickHouse" do
    database = "ecto_ch_migration_test_quoted_ddl"
    opts = [database: database]

    assert :ok = ClickHouse.storage_up(opts)
    on_exit(fn -> ClickHouse.storage_down(opts) end)

    Application.put_env(:migration_test, MigrationRepo,
      database: database,
      show_sensitive_data_on_connection_error: true
    )

    on_exit(fn -> Application.delete_env(:migration_test, MigrationRepo) end)

    start_supervised!(MigrationRepo)

    assert [1] ==
             Ecto.Migrator.run(MigrationRepo, [{1, QuotedDDL}], :up,
               all: true,
               log: false
             )

    conn = start_supervised!({Ch, opts})
    table = QuotedDDL.table_name()
    column = QuotedDDL.column_name()
    escaped_text = QuotedDDL.escaped_text()
    params = %{"database" => database, "table" => table, "column" => column}

    assert [[^escaped_text]] =
             Ch.query!(
               conn,
               "SELECT comment FROM system.tables WHERE database = {database:String} AND name = {table:String}",
               params
             ).rows

    assert [[^escaped_text]] =
             Ch.query!(
               conn,
               "SELECT comment FROM system.columns WHERE database = {database:String} AND table = {table:String} AND name = {column:String}",
               params
             ).rows

    quoted_table = Connection.quote_name(table)
    quoted_column = Connection.quote_name(column)

    insert_sql =
      IO.iodata_to_binary([
        "INSERT INTO ",
        quoted_table,
        " (\"id\") VALUES (1)"
      ])

    Ch.query!(conn, insert_sql)

    select_sql = IO.iodata_to_binary(["SELECT ", quoted_column, " FROM ", quoted_table])
    result = Ch.query!(conn, select_sql)

    assert result.rows == [[escaped_text]]
  end
end
