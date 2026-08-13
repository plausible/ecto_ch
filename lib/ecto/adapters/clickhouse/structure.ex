defmodule Ecto.Adapters.ClickHouse.Structure do
  @moduledoc false
  alias Ch.Query
  alias Ch.Connection, as: Conn

  @conn Ecto.Adapters.ClickHouse.Connection
  @heredoc_tag ~r/\A[A-Za-z0-9_]*\z/

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
    |> split_statements(sql, false, [])
    |> Enum.reverse()
  end

  # `statement` stays at the start of the current query while the first argument advances.
  # This lets us slice complete queries without rebuilding the input byte by byte.
  defp split_statements(sql, statement, significant?, statements) do
    case sql do
      <<>> ->
        add_statement(statements, statement, significant?)

      <<?;, rest::binary>> ->
        query_size = byte_size(statement) - byte_size(rest) - 1
        query = binary_part(statement, 0, query_size)
        split_statements(rest, rest, false, add_statement(statements, query, significant?))

      <<comment::binary-size(2), rest::binary>> when comment in ["--", "//"] ->
        split_statements(skip_through(rest, "\n"), statement, significant?, statements)

      <<"#", next, rest::binary>> when next in [?\s, ?!] ->
        split_statements(skip_through(rest, "\n"), statement, significant?, statements)

      <<"/*", rest::binary>> ->
        case skip_block_comment(rest, 1) do
          {:ok, rest} -> split_statements(rest, statement, significant?, statements)
          :error -> split_statements(<<>>, statement, true, statements)
        end

      <<quote, rest::binary>> when quote in [?\', ?\", ?`] ->
        split_statements(skip_quoted(rest, quote), statement, true, statements)

      <<opening::binary-size(3), rest::binary>> when opening in ["‘", "“"] ->
        closing = if opening == "‘", do: "’", else: "”"
        split_statements(skip_through(rest, closing), statement, true, statements)

      <<?$, rest::binary>> ->
        rest =
          case skip_heredoc(rest) do
            {:ok, rest} -> rest
            :error -> skip_word(rest)
          end

        split_statements(rest, statement, true, statements)

      <<char, rest::binary>>
      when char in ?a..?z or char in ?A..?Z or char in ?0..?9 or char == ?_ ->
        split_statements(skip_word(rest), statement, true, statements)

      <<char, rest::binary>> when char in [?\s, ?\t, ?\n, ?\r, ?\f, ?\v] ->
        split_statements(rest, statement, significant?, statements)

      <<_char, rest::binary>> ->
        split_statements(rest, statement, true, statements)
    end
  end

  defp skip_quoted(sql, quote) do
    case sql do
      <<>> -> <<>>
      <<?\\, _char, rest::binary>> -> skip_quoted(rest, quote)
      <<^quote, ^quote, rest::binary>> -> skip_quoted(rest, quote)
      <<^quote, rest::binary>> -> rest
      <<_char, rest::binary>> -> skip_quoted(rest, quote)
    end
  end

  defp skip_block_comment(sql, depth) do
    case sql do
      <<>> -> :error
      <<"/*", rest::binary>> -> skip_block_comment(rest, depth + 1)
      <<"*/", rest::binary>> when depth == 1 -> {:ok, rest}
      <<"*/", rest::binary>> -> skip_block_comment(rest, depth - 1)
      <<_char, rest::binary>> -> skip_block_comment(rest, depth)
    end
  end

  defp skip_heredoc(sql) do
    with [tag, contents] <- :binary.split(sql, "$"),
         true <- Regex.match?(@heredoc_tag, tag),
         [_, rest] <- :binary.split(contents, "$" <> tag <> "$") do
      {:ok, rest}
    else
      _ -> :error
    end
  end

  defp skip_through(sql, delimiter) do
    case :binary.split(sql, delimiter) do
      [_, rest] -> rest
      [_] -> <<>>
    end
  end

  defp skip_word(<<char, rest::binary>>)
       when char in ?a..?z or char in ?A..?Z or char in ?0..?9 or char in [?_, ?$] do
    skip_word(rest)
  end

  defp skip_word(rest), do: rest

  defp add_statement(statements, _statement, false), do: statements

  defp add_statement(statements, statement, true) do
    case String.trim(statement) do
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
