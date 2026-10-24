defmodule Ecto.Adapters.ClickHouse.MutationHooksTest do
  use ExUnit.Case

  import Ecto.Query

  alias Ecto.Adapters.ClickHouse
  alias Ecto.Integration.TestRepo

  defmodule Repo do
    use Ecto.Repo, adapter: Ecto.Adapters.ClickHouse, otp_app: :mutation_hooks_test

    import Ecto.Query

    def default_options(:update_all), do: [tenant_id: 7, settings: [mutations_sync: 2]]
    def default_options(_operation), do: []

    def prepare_query(:alter_update_all, query, opts) do
      tenant_id = Keyword.fetch!(opts, :tenant_id)
      send(self(), {:prepared_mutation, opts})
      {where(query, [row], row.tenant_id == ^tenant_id), opts}
    end

    def prepare_query(_operation, query, opts), do: {query, opts}
  end

  setup do
    suffix = System.unique_integer([:positive, :monotonic])
    database = "ecto_ch_mutation_hooks_#{suffix}"
    prefixed_database = "ecto_ch_mutation_hooks_prefix_#{suffix}"

    connection_opts =
      TestRepo.config()
      |> Keyword.take([
        :hostname,
        :port,
        :scheme,
        :username,
        :password,
        :settings,
        :transport_opts,
        :show_sensitive_data_on_connection_error
      ])

    for database <- [database, prefixed_database] do
      opts = Keyword.put(connection_opts, :database, database)
      :ok = ClickHouse.storage_up(opts)
      on_exit(fn -> ClickHouse.storage_down(opts) end)
    end

    Application.put_env(
      :mutation_hooks_test,
      Repo,
      Keyword.put(connection_opts, :database, database)
    )

    on_exit(fn -> Application.delete_env(:mutation_hooks_test, Repo) end)
    start_supervised!(Repo)

    for database <- [database, prefixed_database] do
      Repo.query!("""
      CREATE TABLE #{database}.mutation_rows (
        tenant_id UInt64,
        id UInt64,
        value UInt64
      ) ENGINE MergeTree ORDER BY (tenant_id, id)
      """)

      Repo.query!("""
      INSERT INTO #{database}.mutation_rows VALUES
      (7, 1, 10), (7, 2, 20), (9, 1, 30), (9, 2, 40)
      """)
    end

    {:ok, database: database, prefixed_database: prefixed_database}
  end

  test "prepare_query combines the tenant filter with the caller's bound filter", context do
    id = 1
    query = from row in "mutation_rows", where: row.id == ^id

    assert {0, nil} = Repo.alter_update_all(query, set: [value: 99])
    assert_receive {:prepared_mutation, opts}
    assert opts[:tenant_id] == 7
    assert opts[:settings] == [mutations_sync: 2]

    assert stored_rows(context.database) == [
             [7, 1, 99],
             [7, 2, 20],
             [9, 1, 30],
             [9, 2, 40]
           ]
  end

  test "query updates and explicit options retain and override update_all defaults", context do
    id = 2
    increment = 5

    query =
      from row in "mutation_rows",
        where: row.id == ^id,
        update: [inc: [value: ^increment]]

    assert {0, nil} = Repo.alter_update_all(query, [], tenant_id: 9)
    assert_receive {:prepared_mutation, opts}
    assert opts[:tenant_id] == 9
    assert opts[:settings] == [mutations_sync: 2]

    assert stored_rows(context.database) == [
             [7, 1, 10],
             [7, 2, 20],
             [9, 1, 30],
             [9, 2, 45]
           ]
  end

  test "the prefix option routes the mutation away from the repository database", context do
    id = 1
    query = from row in "mutation_rows", where: row.id == ^id

    assert {0, nil} =
             Repo.alter_update_all(query, [set: [value: 77]], prefix: context.prefixed_database)

    assert stored_rows(context.database) == initial_rows()

    assert stored_rows(context.prefixed_database) == [
             [7, 1, 77],
             [7, 2, 20],
             [9, 1, 30],
             [9, 2, 40]
           ]
  end

  test "an explicit source prefix takes priority over the prefix option", context do
    prefix = context.prefixed_database
    id = 2

    query =
      from row in "mutation_rows",
        prefix: ^prefix,
        where: row.id == ^id

    assert {0, nil} =
             Repo.alter_update_all(query, [set: [value: 88]], prefix: context.database)

    assert stored_rows(context.database) == initial_rows()

    assert stored_rows(context.prefixed_database) == [
             [7, 1, 10],
             [7, 2, 88],
             [9, 1, 30],
             [9, 2, 40]
           ]
  end

  defp stored_rows(database) do
    Repo.query!(
      "SELECT tenant_id, id, value FROM #{database}.mutation_rows ORDER BY tenant_id, id"
    ).rows
  end

  defp initial_rows, do: [[7, 1, 10], [7, 2, 20], [9, 1, 30], [9, 2, 40]]
end
