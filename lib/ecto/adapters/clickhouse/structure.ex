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
    |> split_statements(sql, false, [])
    |> Enum.reverse()
  end

  # `statement` stays at the start of the current query while the first argument advances.
  # This lets us slice complete queries without rebuilding the input byte by byte.
  defp split_statements(<<>>, statement, significant?, statements) do
    add_statement(statements, statement, significant?)
  end

  defp split_statements(<<?;, rest::binary>>, statement, significant?, statements) do
    statement_size = byte_size(statement) - byte_size(rest) - 1
    <<query::binary-size(^statement_size), _rest::binary>> = statement
    split_statements(rest, rest, false, add_statement(statements, query, significant?))
  end

  defp split_statements(<<"--", rest::binary>>, statement, significant?, statements) do
    split_statements(skip_until(rest, "\n"), statement, significant?, statements)
  end

  defp split_statements(<<"//", rest::binary>>, statement, significant?, statements) do
    split_statements(skip_until(rest, "\n"), statement, significant?, statements)
  end

  defp split_statements(<<"#", next, rest::binary>>, statement, significant?, statements)
       when next in [?\s, ?!] do
    split_statements(skip_until(rest, "\n"), statement, significant?, statements)
  end

  defp split_statements(<<"/*", rest::binary>>, statement, significant?, statements) do
    case skip_block_comment(rest, 1) do
      {:ok, rest} -> split_statements(rest, statement, significant?, statements)
      :error -> split_statements(<<>>, statement, true, statements)
    end
  end

  defp split_statements(<<quote, rest::binary>>, statement, _significant?, statements)
       when quote in [?\', ?\", ?`] do
    split_statements(skip_quoted(rest, quote), statement, true, statements)
  end

  defp split_statements(<<"‘", rest::binary>>, statement, _significant?, statements) do
    split_statements(skip_until(rest, "’"), statement, true, statements)
  end

  defp split_statements(<<"“", rest::binary>>, statement, _significant?, statements) do
    split_statements(skip_until(rest, "”"), statement, true, statements)
  end

  defp split_statements(<<?$, rest::binary>>, statement, _significant?, statements) do
    case skip_heredoc(rest) do
      {:ok, rest} -> split_statements(rest, statement, true, statements)
      :error -> split_statements(skip_word(rest), statement, true, statements)
    end
  end

  defp split_statements(<<char, rest::binary>>, statement, _significant?, statements)
       when char in ?a..?z or char in ?A..?Z or char in ?0..?9 or char == ?_ do
    split_statements(skip_word(rest), statement, true, statements)
  end

  defp split_statements(<<char, rest::binary>>, statement, significant?, statements)
       when char in [?\s, ?\t, ?\n, ?\r, ?\f, ?\v] do
    split_statements(rest, statement, significant?, statements)
  end

  defp split_statements(<<_char, rest::binary>>, statement, _significant?, statements) do
    split_statements(rest, statement, true, statements)
  end

  defp skip_quoted(<<>>, _quote), do: <<>>

  defp skip_quoted(<<?\\, _char, rest::binary>>, quote) do
    skip_quoted(rest, quote)
  end

  defp skip_quoted(<<quote, quote, rest::binary>>, quote) do
    skip_quoted(rest, quote)
  end

  defp skip_quoted(<<quote, rest::binary>>, quote), do: rest
  defp skip_quoted(<<_char, rest::binary>>, quote), do: skip_quoted(rest, quote)

  defp skip_block_comment(<<>>, _depth), do: :error

  defp skip_block_comment(<<"/*", rest::binary>>, depth) do
    skip_block_comment(rest, depth + 1)
  end

  defp skip_block_comment(<<"*/", rest::binary>>, 1), do: {:ok, rest}

  defp skip_block_comment(<<"*/", rest::binary>>, depth) do
    skip_block_comment(rest, depth - 1)
  end

  defp skip_block_comment(<<_char, rest::binary>>, depth) do
    skip_block_comment(rest, depth)
  end

  defp skip_heredoc(sql) do
    with {tag_size, 1} <- :binary.match(sql, "$"),
         <<tag::binary-size(^tag_size), ?$, contents::binary>> <- sql,
         true <- valid_heredoc_tag?(tag),
         delimiter = "$" <> tag <> "$",
         {position, size} <- :binary.match(contents, delimiter),
         rest_position = position + size,
         rest = binary_part(contents, rest_position, byte_size(contents) - rest_position) do
      {:ok, rest}
    else
      _ -> :error
    end
  end

  defp skip_until(sql, delimiter) do
    case :binary.match(sql, delimiter) do
      {position, size} ->
        rest_position = position + size
        binary_part(sql, rest_position, byte_size(sql) - rest_position)

      :nomatch ->
        <<>>
    end
  end

  defp valid_heredoc_tag?(<<>>), do: true

  defp valid_heredoc_tag?(<<char, rest::binary>>)
       when char in ?a..?z or char in ?A..?Z or char in ?0..?9 or char == ?_ do
    valid_heredoc_tag?(rest)
  end

  defp valid_heredoc_tag?(_tag), do: false

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
