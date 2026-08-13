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

  # `statement` stays at the start of the current query while `sql` advances.
  # Complete queries can therefore be sliced without rebuilding the input.
  defp split_statements(<<>>, statement, significant?, statements),
    do: add_statement(statements, statement, significant?)

  defp split_statements(<<?;, rest::binary>>, statement, significant?, statements) do
    query_size = byte_size(statement) - byte_size(rest) - 1
    query = binary_part(statement, 0, query_size)
    statements = add_statement(statements, query, significant?)
    split_statements(rest, rest, false, statements)
  end

  defp split_statements(
         <<comment::binary-size(2), rest::binary>>,
         statement,
         significant?,
         statements
       )
       when comment in ["--", "//", "# ", "#!"] do
    rest = skip_through(rest, "\n")
    split_statements(rest, statement, significant?, statements)
  end

  defp split_statements(<<"/*", rest::binary>>, statement, significant?, statements) do
    case skip_block_comment(rest) do
      {:ok, rest} -> split_statements(rest, statement, significant?, statements)
      :error -> split_statements(<<>>, statement, true, statements)
    end
  end

  defp split_statements(<<quote, rest::binary>>, statement, _significant?, statements)
       when quote in [?\', ?\", ?`] do
    rest = skip_quoted(rest, quote)
    split_statements(rest, statement, true, statements)
  end

  defp split_statements(<<"‘", rest::binary>>, statement, _significant?, statements),
    do: split_statements(skip_through(rest, "’"), statement, true, statements)

  defp split_statements(<<"“", rest::binary>>, statement, _significant?, statements),
    do: split_statements(skip_through(rest, "”"), statement, true, statements)

  defp split_statements(<<?$, rest::binary>>, statement, _significant?, statements) do
    rest =
      case skip_heredoc(rest) do
        {:ok, rest} -> rest
        :error -> skip_bare_word(rest)
      end

    split_statements(rest, statement, true, statements)
  end

  defp split_statements(<<char, rest::binary>>, statement, _significant?, statements)
       when char in ?a..?z or char in ?A..?Z or char == ?_ do
    split_statements(skip_bare_word(rest), statement, true, statements)
  end

  # Number-start tokens do not absorb `$`, so it remains a possible heredoc boundary.
  defp split_statements(<<char, rest::binary>>, statement, _significant?, statements)
       when char in ?0..?9 do
    split_statements(skip_number_word(rest), statement, true, statements)
  end

  defp split_statements(<<char, rest::binary>>, statement, significant?, statements)
       when char in [?\s, ?\t, ?\n, ?\r, ?\f, ?\v] do
    split_statements(rest, statement, significant?, statements)
  end

  defp split_statements(<<_char, rest::binary>>, statement, _significant?, statements),
    do: split_statements(rest, statement, true, statements)

  defp skip_quoted(<<>>, _quote), do: <<>>

  defp skip_quoted(<<?\\, _escaped, rest::binary>>, quote),
    do: skip_quoted(rest, quote)

  defp skip_quoted(<<quote, quote, rest::binary>>, quote),
    do: skip_quoted(rest, quote)

  defp skip_quoted(<<quote, rest::binary>>, quote), do: rest

  defp skip_quoted(<<_char, rest::binary>>, quote),
    do: skip_quoted(rest, quote)

  defp skip_block_comment(sql), do: skip_block_comment(sql, 1)

  defp skip_block_comment(<<>>, _depth), do: :error

  defp skip_block_comment(<<"/*", rest::binary>>, depth),
    do: skip_block_comment(rest, depth + 1)

  defp skip_block_comment(<<"*/", rest::binary>>, 1), do: {:ok, rest}

  defp skip_block_comment(<<"*/", rest::binary>>, depth),
    do: skip_block_comment(rest, depth - 1)

  defp skip_block_comment(<<_char, rest::binary>>, depth),
    do: skip_block_comment(rest, depth)

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

  defp skip_bare_word(<<char, rest::binary>>)
       when char in ?a..?z or char in ?A..?Z or char in ?0..?9 or char in [?_, ?$],
       do: skip_bare_word(rest)

  defp skip_bare_word(rest), do: rest

  defp skip_number_word(<<char, rest::binary>>)
       when char in ?a..?z or char in ?A..?Z or char in ?0..?9 or char == ?_,
       do: skip_number_word(rest)

  defp skip_number_word(rest), do: rest

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
