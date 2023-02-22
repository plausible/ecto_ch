Logger.configure(level: :info)

Application.put_env(:ecto, :primary_key_type, :id)
Application.put_env(:ecto, :async_integration_tests, false)

ecto = Mix.Project.deps_paths()[:ecto]
ecto_sql = Mix.Project.deps_paths()[:ecto_sql]

Code.require_file("#{ecto_sql}/integration_test/support/repo.exs", __DIR__)

alias Ecto.Integration.TestRepo

Application.put_env(:chto, TestRepo,
  adapter: Ecto.Adapters.ClickHouse,
  database: "chot_integration_test",
  show_sensitive_data_on_connection_error: true
)

# Pool repo for non-async tests
alias Ecto.Integration.PoolRepo

Application.put_env(:chto, PoolRepo,
  adapter: Ecto.Adapters.ClickHouse,
  database: "chto_integration_pool_test",
  show_sensitive_data_on_connection_error: true
)

# needed since some of the integration tests rely on fetching env from :ecto_sql
Application.put_env(:ecto_sql, TestRepo, Application.get_env(:chto, TestRepo))
Application.put_env(:ecto_sql, PoolRepo, Application.get_env(:chto, PoolRepo))

defmodule Ecto.Integration.PoolRepo do
  use Ecto.Integration.Repo, otp_app: :chto, adapter: Ecto.Adapters.ClickHouse
end

Code.require_file("#{ecto}/integration_test/support/schemas.exs", __DIR__)
Code.require_file("#{ecto_sql}/integration_test/support/migration.exs", __DIR__)

{:ok, _} = Ecto.Adapters.ClickHouse.ensure_all_started(TestRepo.config(), :temporary)

# Load up the repository, start it, and run migrations
_ = Ecto.Adapters.ClickHouse.storage_down(TestRepo.config())
:ok = Ecto.Adapters.ClickHouse.storage_up(TestRepo.config())

_ = Ecto.Adapters.ClickHouse.storage_down(PoolRepo.config())
:ok = Ecto.Adapters.ClickHouse.storage_up(PoolRepo.config())

{:ok, _} = TestRepo.start_link()
{:ok, _pid} = PoolRepo.start_link()

# migrate the pool repo
case Ecto.Migrator.migrated_versions(PoolRepo) do
  [] ->
    :ok = Ecto.Migrator.up(PoolRepo, 0, Ecto.Integration.Migration, log: false)

  _ ->
    :ok = Ecto.Migrator.down(PoolRepo, 0, Ecto.Integration.Migration, log: false)
    :ok = Ecto.Migrator.up(PoolRepo, 0, Ecto.Integration.Migration, log: false)
end

:ok = Ecto.Migrator.up(TestRepo, 0, Ecto.Integration.Migration, log: false)

Process.flag(:trap_exit, true)
ExUnit.start()
