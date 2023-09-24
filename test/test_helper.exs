Logger.configure(level: :info)
Calendar.put_time_zone_database(Tz.TimeZoneDatabase)

Code.require_file("test/support/ecto_schemas.exs")
Code.require_file("test/support/schemas.exs")

alias Ecto.Integration.TestRepo

Application.put_env(:ecto_ch, TestRepo,
  adapter: Ecto.Adapters.ClickHouse,
  database: "ecto_ch_test",
  settings: [path: "./.ch"],
  show_sensitive_data_on_connection_error: true,
  pool_size: 1,
  cmd: Ch.Local.clickhouse_local_cmd()
)

{:ok, _} = Ecto.Adapters.ClickHouse.ensure_all_started(TestRepo.config(), :temporary)

_ = Ecto.Adapters.ClickHouse.storage_down(TestRepo.config())
:ok = Ecto.Adapters.ClickHouse.storage_up(TestRepo.config())

{:ok, _} = TestRepo.start_link()

:ok =
  Ecto.Migrator.up(TestRepo, 0, EctoClickHouse.Integration.Migration,
    log: false,
    prefix: "ecto_ch_test"
  )

ExUnit.start()
