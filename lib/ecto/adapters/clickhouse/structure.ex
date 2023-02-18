defmodule Ecto.Adapters.ClickHouse.Structure do
  @moduledoc false
  alias Ch.Query
  alias Ch.Connection, as: Conn

  @conn Ecto.Adapters.ClickHouse.Connection

  def structure_dump(default, config) do
    path = config[:dump_path] || Path.join(default, "structure.sql")
    migration_source = config[:migration_source] || "schema_migrations"

    with {:ok, conn} <- Conn.connect(config),
         {:ok, contents, conn} <- structure_dump_schema(conn),
         {:ok, versions, _conn} <- structure_dump_versions(conn, migration_source) do
      File.mkdir_p!(Path.dirname(path))
      File.write!(path, [contents, ?\n, versions])
      {:ok, path}
    end
  end

  # TODO show dictionaries, views
  defp structure_dump_schema(conn) do
    with {:ok, %{rows: rows}, conn} <- exec(conn, "SHOW TABLES") do
      tables = Enum.map(rows, fn [table] -> table end)
      structure_dump_tables(conn, tables)
    end
  end

  defp structure_dump_tables(conn, tables) do
    stmt = fn table -> "SHOW CREATE TABLE #{@conn.quote_name(table)}" end

    result =
      Enum.reduce_while(tables, {[], conn}, fn table, {schemas, conn} ->
        case exec(conn, stmt.(table)) do
          {:ok, %{rows: [[schema]]}, conn} -> {:cont, {[schema, ?\n | schemas], conn}}
          {:error, _reason} = error -> {:halt, error}
        end
      end)

    case result do
      {:error, _reason} = error -> error
      schemas when is_list(schemas) -> {:ok, schemas, conn}
    end
  end

  defp structure_dump_versions(conn, table) do
    table = @conn.quote_name(table)
    stmt = "SELECT * FROM #{table} FORMAT CSVWithNames"

    with {:ok, %{rows: rows}, conn} <- exec(conn, stmt, [], format: "Values") do
      versions = ["INSERT INTO ", table, "(version, inserted_at) VALUES " | rows]
      {:ok, versions, conn}
    end
  end

  def exec(conn, sql, params \\ [], opts \\ []) do
    {query_opts, exec_opts} = Keyword.split(opts, [:command])
    query = Query.build(sql, query_opts)

    case Conn.handle_execute(query, params, exec_opts, conn) do
      {:ok, _query, result, conn} -> {:ok, result, conn}
      {:disconnect, reason, _conn} -> {:error, reason}
      {:error, reason, _conn} -> {:error, reason}
    end
  end
end
