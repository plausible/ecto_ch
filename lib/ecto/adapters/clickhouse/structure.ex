defmodule Ecto.Adapters.ClickHouse.Structure do
  @moduledoc false
  alias Ch.Query
  alias Ch.Connection, as: Conn

  @conn Ecto.Adapters.ClickHouse.Connection

  # TODO include views

  def structure_dump(default, config) do
    path = config[:dump_path] || Path.join(default, "structure.sql")
    migration_source = config[:migration_source] || "schema_migrations"

    with {:ok, conn} <- Conn.connect(config),
         {:ok, tables, conn} <- show("TABLES", conn),
         {:ok, dicts, conn} <- show("DICTIONARIES", conn),
         tables = tables -- [migration_source],
         {:ok, tables, conn} <- show_create("TABLE", conn, [migration_source | tables]),
         {:ok, dicts, conn} <- show_create("DICTIONARY", conn, dicts),
         {:ok, versions, _conn} <- dump_versions(conn, migration_source) do
      File.mkdir_p!(Path.dirname(path))
      File.write!(path, [tables, dicts, versions])
      {:ok, path}
    end
  end

  defp show(what, conn) do
    with {:ok, %{rows: rows}, conn} <- exec(conn, "SHOW #{what}") do
      objects = Enum.map(rows, fn [object] -> object end)
      {:ok, objects, conn}
    end
  end

  defp show_create(what, conn, objects) do
    show = fn object -> "SHOW CREATE #{what} #{@conn.quote_name(object)}" end

    result =
      Enum.reduce_while(objects, {[], conn}, fn object, {schemas, conn} ->
        case exec(conn, show.(object)) do
          {:ok, %{rows: [[schema]]}, conn} -> {:cont, {[schema, ";\n\n" | schemas], conn}}
          {:error, _reason} = error -> {:halt, error}
        end
      end)

    case result do
      {:error, _reason} = error -> error
      {schemas, conn} when is_list(schemas) -> {:ok, schemas, conn}
    end
  end

  defp dump_versions(conn, table) do
    table = @conn.quote_name(table)
    stmt = "SELECT * FROM #{table}"

    with {:ok, %{rows: rows}, conn} <- exec(conn, stmt, [], format: "Values") do
      rows = rows |> IO.iodata_to_binary() |> String.replace("),(", "),\n(")
      versions = ["INSERT INTO ", table, "(version, inserted_at) VALUES\n", rows, ";\n"]
      {:ok, versions, conn}
    end
  end

  def exec(conn, sql, params \\ [], opts \\ []) do
    query = Query.build(sql)

    case Conn.handle_execute(query, params, opts, conn) do
      {:ok, _query, result, conn} -> {:ok, result, conn}
      {:disconnect, reason, _conn} -> {:error, reason}
      {:error, reason, _conn} -> {:error, reason}
    end
  end
end
