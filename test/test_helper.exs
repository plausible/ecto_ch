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

%{rows: [[ch_version]]} =
  Task.async(fn ->
    {:ok, pid} = Ch.start_link()
    Ch.query!(pid, "select version()")
  end)
  |> Task.await()

alias Ecto.Integration.TestRepo

env = [
  adapter: Ecto.Adapters.ClickHouse,
  database: "ecto_ch_test",
  show_sensitive_data_on_connection_error: true
]

env =
  if ch_version >= "25" do
    Keyword.put(env, :settings, enable_json_type: 1)
  else
    env
  end

Application.put_env(:ecto_ch, TestRepo, env)

exclude =
  if ch_version >= "25" do
    []
  else
    # these types are not supported in older ClickHouse versions we have in the CI
    [:time, :variant, :json, :dynamic, :lightweight_delete]
  end

{:ok, _} = Ecto.Adapters.ClickHouse.ensure_all_started(TestRepo.config(), :temporary)

_ = Ecto.Adapters.ClickHouse.storage_down(TestRepo.config())
:ok = Ecto.Adapters.ClickHouse.storage_up(TestRepo.config())

{:ok, _} = TestRepo.start_link()
:ok = Ecto.Migrator.up(TestRepo, 0, EctoClickHouse.Integration.Migration, log: false)

ExUnit.start(exclude: exclude)
