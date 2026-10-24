defmodule Ecto.Adapters.ClickHouse.MigrationLifecycleTest do
  use ExUnit.Case

  alias Ecto.Adapters.ClickHouse
  alias Ecto.Integration.TestRepo

  defmodule Repo do
    use Ecto.Repo, adapter: Ecto.Adapters.ClickHouse, otp_app: :migration_lifecycle_test
  end

  defmodule CreateCheckedValues do
    use Ecto.Migration

    def change do
      create table(:checked_values,
               primary_key: false,
               engine: "MergeTree",
               options: "ORDER BY tuple()"
             ) do
        add :value, :UInt8
      end

      create constraint(:checked_values, :positive_value, check: "value > 0", validate: false)
    end
  end

  defmodule DropPositiveValueConstraint do
    use Ecto.Migration

    def change do
      drop constraint(:checked_values, :positive_value)
    end
  end

  defmodule CreateRenameValues do
    use Ecto.Migration

    def change do
      create table(:values_before_rename,
               primary_key: false,
               engine: "MergeTree",
               options: "ORDER BY tuple()"
             ) do
        add :value, :UInt8
        add :label, :string
      end
    end
  end

  defmodule RenameValuesTable do
    use Ecto.Migration

    def change do
      rename table(:values_before_rename), to: table(:values_after_rename)
    end
  end

  defmodule RenameValueColumn do
    use Ecto.Migration

    def change do
      rename table(:values_after_rename), :value, to: :amount
    end
  end

  defmodule WidenAmountColumn do
    use Ecto.Migration

    def change do
      alter table(:values_after_rename) do
        modify :amount, :UInt16, from: :UInt8
      end
    end
  end

  setup do
    config = TestRepo.config()

    database =
      "ecto_ch_migration_lifecycle_#{System.system_time(:microsecond)}_#{System.unique_integer([:positive])}"

    opts =
      config
      |> Keyword.take([:hostname, :username, :password, :scheme, :settings, :transport_opts])
      |> Keyword.put(:database, database)
      |> Keyword.put(:port, config[:port] || 8123)

    assert :ok = ClickHouse.storage_up(opts)
    on_exit(fn -> ClickHouse.storage_down(opts) end)

    previous_config = Application.get_env(:migration_lifecycle_test, Repo)
    Application.put_env(:migration_lifecycle_test, Repo, opts)

    on_exit(fn ->
      if previous_config do
        Application.put_env(:migration_lifecycle_test, Repo, previous_config)
      else
        Application.delete_env(:migration_lifecycle_test, Repo)
      end
    end)

    start_supervised!(Repo)
    :ok
  end

  test "check constraints reject invalid inserts until dropped by a migration" do
    migrate(1, CreateCheckedValues)

    Repo.query!("INSERT INTO checked_values (value) VALUES (1)")

    assert_raise Ch.Error, ~r/CONSTRAINT/, fn ->
      Repo.query!("INSERT INTO checked_values (value) VALUES (0)")
    end

    assert Repo.query!("SELECT value FROM checked_values").rows == [[1]]

    migrate(2, DropPositiveValueConstraint)

    Repo.query!("INSERT INTO checked_values (value) VALUES (0)")
    assert Repo.query!("SELECT value FROM checked_values ORDER BY value").rows == [[0], [1]]
  end

  test "table rename, column rename, and type widening preserve existing rows" do
    migrate(1, CreateRenameValues)

    Repo.query!("""
    INSERT INTO values_before_rename (value, label) VALUES (0, 'zero'), (255, 'maximum')
    """)

    expected = [[0, "zero"], [255, "maximum"]]

    migrate(2, RenameValuesTable)

    refute Ecto.Adapters.SQL.table_exists?(Repo, "values_before_rename")

    assert Repo.query!("SELECT value, label FROM values_after_rename ORDER BY value").rows ==
             expected

    migrate(3, RenameValueColumn)

    assert Repo.query!("SELECT amount, label FROM values_after_rename ORDER BY amount").rows ==
             expected

    migrate(4, WidenAmountColumn)

    assert Repo.query!("SELECT amount, label FROM values_after_rename ORDER BY amount").rows ==
             expected

    Repo.query!("INSERT INTO values_after_rename (amount, label) VALUES (300, 'wider')")

    assert Repo.query!("SELECT amount, label FROM values_after_rename ORDER BY amount").rows ==
             expected ++ [[300, "wider"]]
  end

  defp migrate(version, migration) do
    assert [^version] =
             Ecto.Migrator.run(Repo, [{version, migration}], :up, all: true, log: false)
  end
end
