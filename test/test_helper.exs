clickhouse_available? =
  case :httpc.request(:get, {~c"http://localhost:8123/ping", []}, [], []) do
    {:ok, {{_version, _status = 200, _reason}, _headers, ~c"Ok.\n"}} ->
      true

    {:error, {:failed_connect, [{:to_address, _to_address}, {:inet, [:inet], :econnrefused}]}} ->
      false
  end

unless clickhouse_available? do
  Mix.shell().error("""
  ClickHouse is not detected! Please start the local container with the following command:

      docker compose up -d clickhouse
  """)

  System.halt(1)
end

Logger.configure(level: :info)
Calendar.put_time_zone_database(Tz.TimeZoneDatabase)

Code.require_file("test/support/ecto_schemas.exs")
Code.require_file("test/support/schemas.exs")

alias Ecto.Integration.TestRepo

Application.put_env(:ecto_ch, TestRepo,
  adapter: Ecto.Adapters.ClickHouse,
  database: "ecto_ch_test",
  settings: [enable_json_type: 1],
  show_sensitive_data_on_connection_error: true
)

{:ok, _} = Ecto.Adapters.ClickHouse.ensure_all_started(TestRepo.config(), :temporary)

_ = Ecto.Adapters.ClickHouse.storage_down(TestRepo.config())
:ok = Ecto.Adapters.ClickHouse.storage_up(TestRepo.config())

{:ok, _} = TestRepo.start_link()
:ok = Ecto.Migrator.up(TestRepo, 0, EctoClickHouse.Integration.Migration, log: false)

%{rows: [[ch_version]]} = TestRepo.query!("SELECT version()")

exclude =
  if ch_version >= "25" do
    []
  else
    # Time type is not supported in older ClickHouse versions we have in the CI
    [:time]
  end

ExUnit.start(exclude: exclude)
