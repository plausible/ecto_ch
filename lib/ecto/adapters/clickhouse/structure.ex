defmodule Ecto.Adapters.ClickHouse.Structure do
  @moduledoc false
  alias Ch.Query
  alias Ch.Connection, as: Conn

  @conn Ecto.Adapters.ClickHouse.Connection

  def structure_load(default, config) do
    path = config[:dump_path] || Path.join(default, "structure.sql")

    with {:ok, conn} <- Conn.connect(config),
         {:ok, queries} <- File.read(path) do
      multiquery_result =
        queries
        |> split_statements()
        |> Enum.reduce_while({:ok, _prev_result = nil, conn}, fn
          query, {:ok, _prev_result, conn} -> {:cont, exec(conn, query)}
          _query, {:error, _reason} = error -> {:halt, error}
        end)

      case multiquery_result do
        {:ok, _last_result, _conn} -> {:ok, path}
        {:error, reason} -> {:error, Exception.message(reason)}
      end
    end
  end

  defp split_statements(sql) do
    sql
    |> split_statements(:sql, [], [])
    |> Enum.reverse()
  end

  defp split_statements(<<>>, _state, statement, statements) do
    add_statement(statements, statement)
  end

  defp split_statements(<<?;, rest::binary>>, :sql, statement, statements) do
    split_statements(rest, :sql, [], add_statement(statements, statement))
  end

  defp split_statements(<<"--", rest::binary>>, :sql, statement, statements) do
    split_statements(rest, :line_comment, ["--" | statement], statements)
  end

  defp split_statements(<<?#, rest::binary>>, :sql, statement, statements) do
    split_statements(rest, :line_comment, [?# | statement], statements)
  end

  defp split_statements(<<"/*", rest::binary>>, :sql, statement, statements) do
    split_statements(rest, :block_comment, ["/*" | statement], statements)
  end

  defp split_statements(<<quote, rest::binary>>, :sql, statement, statements)
       when quote in [?\', ?\", ?`] do
    split_statements(rest, {:quoted, quote}, [quote | statement], statements)
  end

  defp split_statements(
         <<?\\, char, rest::binary>>,
         {:quoted, _quote} = state,
         statement,
         statements
       ) do
    split_statements(rest, state, [char, ?\\ | statement], statements)
  end

  defp split_statements(
         <<quote, quote, rest::binary>>,
         {:quoted, quote} = state,
         statement,
         statements
       ) do
    split_statements(rest, state, [quote, quote | statement], statements)
  end

  defp split_statements(<<quote, rest::binary>>, {:quoted, quote}, statement, statements) do
    split_statements(rest, :sql, [quote | statement], statements)
  end

  defp split_statements(<<?\n, rest::binary>>, :line_comment, statement, statements) do
    split_statements(rest, :sql, [?\n | statement], statements)
  end

  defp split_statements(<<"*/", rest::binary>>, :block_comment, statement, statements) do
    split_statements(rest, :sql, ["*/" | statement], statements)
  end

  defp split_statements(<<char, rest::binary>>, state, statement, statements) do
    split_statements(rest, state, [char | statement], statements)
  end

  defp add_statement(statements, statement) do
    case statement |> Enum.reverse() |> IO.iodata_to_binary() |> String.trim() do
      "" -> statements
      statement -> [statement | statements]
    end
  end

  # TODO include views

  def structure_dump(default, config) do
    path = config[:dump_path] || Path.join(default, "structure.sql")
    migration_source = config[:migration_source] || "schema_migrations"
    database = config[:database] || "default"

    with {:ok, conn} <- Conn.connect(config),
         {:ok, tables, conn} <- show("TABLES", conn),
         {:ok, dicts, conn} <- show("DICTIONARIES", conn),
         tables = tables -- [migration_source],
         {:ok, tables, conn} <- show_create("TABLE", conn, [migration_source | tables]),
         {:ok, dicts, conn} <- show_create("DICTIONARY", conn, dicts),
         {:ok, versions, _conn} <- dump_versions(conn, database, migration_source) do
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

  defp dump_versions(conn, database, table) do
    table = @conn.quote_table(database, table)
    stmt = "SELECT * FROM #{table} FORMAT Values"

    with {:ok, %{rows: rows}, conn} <- exec(conn, stmt) do
      versions =
        case IO.iodata_to_binary(rows) do
          "" ->
            []

          rows ->
            rows = String.replace(rows, "),(", "),\n(")
            ["INSERT INTO ", table, " (version, inserted_at) VALUES\n", rows, ";\n"]
        end

      {:ok, versions, conn}
    end
  end

  def exec(conn, sql, params \\ [], opts \\ []) do
    query = Query.build(sql)
    params = DBConnection.Query.encode(query, params, [])

    case Conn.handle_execute(query, params, opts, conn) do
      {:ok, query, result, conn} -> {:ok, DBConnection.Query.decode(query, result, []), conn}
      {:disconnect, reason, _conn} -> {:error, reason}
      {:error, reason, _conn} -> {:error, reason}
    end
  end
end
