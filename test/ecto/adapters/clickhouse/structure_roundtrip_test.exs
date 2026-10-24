defmodule Ecto.Adapters.ClickHouse.StructureRoundtripTest do
  use ExUnit.Case

  alias Ecto.Adapters.ClickHouse
  alias Ecto.Integration.TestRepo

  defmodule Repo do
    use Ecto.Repo, adapter: Ecto.Adapters.ClickHouse, otp_app: :structure_roundtrip_test
  end

  defmodule CreateItems do
    use Ecto.Migration

    def change do
      create table(:restored_items, primary_key: false, engine: "Memory") do
        add :id, :UInt64
        add :name, :string
      end
    end
  end

  defmodule AddLabel do
    use Ecto.Migration

    def change do
      alter table(:restored_items) do
        add :label, :string, default: "restored"
      end
    end
  end

  setup tags do
    database = "ecto_ch_structure_roundtrip_#{System.unique_integer([:positive])}"
    tmp = Path.join(System.tmp_dir!(), database)

    opts = [
      database: database,
      port: TestRepo.config()[:port] || 8123,
      migration_source: tags[:migration_source] || "schema_migrations",
      dump_path: Path.join([tmp, "nested", "structure.sql"]),
      settings: [async_insert: 0]
    ]

    :ok = ClickHouse.storage_up(opts)
    Application.put_env(:structure_roundtrip_test, Repo, opts)

    on_exit(fn ->
      ClickHouse.storage_down(opts)
      Application.delete_env(:structure_roundtrip_test, Repo)
      File.rm_rf!(tmp)
    end)

    start_supervised!(Repo)
    {:ok, opts: opts, path: opts[:dump_path], tmp: tmp}
  end

  for migration_source <- ["schema_migrations", "custom_history"] do
    @tag migration_source: migration_source
    test "restores nonempty migration history using #{migration_source}", context do
      %{opts: opts, path: path, tmp: tmp} = context

      assert [11, 22] =
               Ecto.Migrator.run(Repo, [{11, CreateItems}, {22, AddLabel}], :up,
                 all: true,
                 log: false
               )

      assert {:ok, ^path} = ClickHouse.structure_dump(tmp, opts)
      assert File.regular?(path)

      assert :ok = ClickHouse.storage_down(opts)
      assert :ok = ClickHouse.storage_up(opts)
      assert {:ok, ^path} = ClickHouse.structure_load(tmp, opts)

      assert Enum.sort(Ecto.Migrator.migrated_versions(Repo)) == [11, 22]
      assert :already_up = Ecto.Migrator.up(Repo, 11, CreateItems, log: false)
      assert :already_up = Ecto.Migrator.up(Repo, 22, AddLabel, log: false)

      Repo.query!("INSERT INTO restored_items (id, name) VALUES (7, 'after restore')")

      assert Repo.query!("SELECT id, name, label FROM restored_items").rows ==
               [[7, "after restore", "restored"]]
    end
  end

  test "stops restoring after the first failing statement", %{opts: opts, path: path, tmp: tmp} do
    File.mkdir_p!(Path.dirname(path))

    File.write!(path, """
    CREATE TABLE before_failure (id UInt8) ENGINE Memory;
    THIS IS INVALID SQL;
    CREATE TABLE after_failure (id UInt8) ENGINE Memory;
    """)

    assert {:error, message} = ClickHouse.structure_load(tmp, opts)
    assert message =~ "SYNTAX_ERROR"
    assert Repo.query!("EXISTS TABLE before_failure").rows == [[1]]
    assert Repo.query!("EXISTS TABLE after_failure").rows == [[0]]

    Repo.query!("INSERT INTO before_failure VALUES (42)")
    assert Repo.query!("SELECT id FROM before_failure").rows == [[42]]
  end

  test "reports a missing dump file", %{opts: opts, path: path, tmp: tmp} do
    refute File.exists?(path)
    assert {:error, :enoent} = ClickHouse.structure_load(tmp, opts)
  end
end
