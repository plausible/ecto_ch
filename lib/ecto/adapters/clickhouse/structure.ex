defmodule Ecto.Adapters.ClickHouse.Structure do
  @moduledoc false

  @conn Ecto.Adapters.ClickHouse.Connection

  def structure_load(default, config) do
    path = config[:dump_path] || Path.join(default, "structure.sql")

    with {:ok, queries} <- File.read(path) do
      with_pool(config, fn conn, opts ->
        multiquery_result =
          queries
          |> String.split(";", trim: true)
          |> Enum.map(&String.trim/1)
          |> Enum.reject(&(&1 == ""))
          |> Enum.reduce_while({:ok, _prev_result = nil}, fn
            query, {:ok, _prev_result} -> {:cont, exec(conn, query, [], opts)}
            _query, {:error, _reason} = error -> {:halt, error}
          end)

        case multiquery_result do
          {:ok, _last_result} -> {:ok, path}
          {:error, reason} -> {:error, Exception.message(reason)}
        end
      end)
    end
  end

  # TODO include views

  def structure_dump(default, config) do
    path = config[:dump_path] || Path.join(default, "structure.sql")
    migration_source = config[:migration_source] || "schema_migrations"
    database = config[:database] || "default"

    with_pool(config, fn conn, opts ->
      with {:ok, tables} <- show("TABLES", conn, opts),
           {:ok, dicts} <- show("DICTIONARIES", conn, opts),
           tables = tables -- [migration_source],
           {:ok, tables} <- show_create("TABLE", conn, opts, [migration_source | tables]),
           {:ok, dicts} <- show_create("DICTIONARY", conn, opts, dicts),
           {:ok, versions} <- dump_versions(conn, opts, database, migration_source) do
        File.mkdir_p!(Path.dirname(path))
        File.write!(path, [tables, dicts, versions])
        {:ok, path}
      end
    end)
  end

  defp show(what, conn, opts) do
    with {:ok, %{rows: rows}} <- exec(conn, "SHOW #{what}", [], opts) do
      objects = Enum.map(rows, fn [object] -> object end)
      {:ok, objects}
    end
  end

  defp show_create(what, conn, opts, objects) do
    show = fn object -> "SHOW CREATE #{what} #{@conn.quote_name(object)}" end

    result =
      Enum.reduce_while(objects, [], fn object, schemas ->
        case exec(conn, show.(object), [], opts) do
          {:ok, %{rows: [[schema]]}} -> {:cont, [schema, ";\n\n" | schemas]}
          {:error, _reason} = error -> {:halt, error}
        end
      end)

    case result do
      {:error, _reason} = error -> error
      schemas when is_list(schemas) -> {:ok, schemas}
    end
  end

  defp dump_versions(conn, opts, database, table) do
    table = @conn.quote_table(database, table)
    stmt = "SELECT * FROM #{table} FORMAT Values"

    with {:ok, %{rows: rows}} <- exec(conn, stmt, [], opts) do
      rows = rows |> IO.iodata_to_binary() |> String.replace("),(", "),\n(")
      versions = ["INSERT INTO ", table, " (version, inserted_at) VALUES\n", rows, ";\n"]
      {:ok, versions}
    end
  end

  defp with_pool(config, fun) do
    case Ch.start_link(@conn.start_options(config)) do
      {:ok, conn} ->
        try do
          fun.(conn, @conn.config_options(config))
        after
          Ch.stop(conn)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  def exec(conn, sql, params \\ [], opts \\ []) do
    @conn.query(conn, sql, params, opts)
  end
end
