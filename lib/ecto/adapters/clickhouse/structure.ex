defmodule Ecto.Adapters.ClickHouse.Structure do
  @moduledoc false
  alias Ch.Query
  alias Ch.Connection, as: Conn

  @conn Ecto.Adapters.ClickHouse.Connection
  @statement_markers [";", "--", "//", "/*", "# ", "#!", "'", "\"", "`", "$", "‘", "“"]
  @heredoc_tag ~r/\A[A-Za-z0-9_]*\z/
  @bare_word ~r/\A[A-Za-z0-9_$]+/
  @bare_word_before_dollar ~r/(?:\A|[^A-Za-z0-9_])[A-Za-z_][A-Za-z0-9_]*\z/

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
    markers = :binary.compile_pattern(@statement_markers)

    sql
    |> split_statements(sql, false, [], markers)
    |> Enum.reverse()
  end

  # `statement` stays at the start of the current query while the first argument advances.
  # Complete queries can therefore be sliced without rebuilding the input.
  defp split_statements(sql, statement, significant?, statements, markers) do
    case :binary.match(sql, markers) do
      :nomatch ->
        add_statement(statements, statement, significant? or has_sql?(sql))

      {offset, size} ->
        <<prefix::binary-size(^offset), marker::binary-size(^size), rest::binary>> = sql
        significant? = significant? or has_sql?(prefix)

        split_at(marker, rest, prefix, statement, significant?, statements, markers)
    end
  end

  defp split_at(";", rest, _prefix, statement, significant?, statements, markers) do
    query_size = byte_size(statement) - byte_size(rest) - 1
    query = binary_part(statement, 0, query_size)
    statements = add_statement(statements, query, significant?)
    split_statements(rest, rest, false, statements, markers)
  end

  defp split_at(comment, rest, _prefix, statement, significant?, statements, markers)
       when comment in ["--", "//", "# ", "#!"] do
    rest = skip_through(rest, "\n")
    split_statements(rest, statement, significant?, statements, markers)
  end

  defp split_at("/*", rest, _prefix, statement, significant?, statements, markers) do
    case skip_block_comment(rest) do
      {:ok, rest} -> split_statements(rest, statement, significant?, statements, markers)
      :error -> split_statements(<<>>, statement, true, statements, markers)
    end
  end

  defp split_at(quote, rest, _prefix, statement, _significant?, statements, markers)
       when quote in ["'", "\"", "`"] do
    rest = skip_quoted(rest, :binary.first(quote))
    split_statements(rest, statement, true, statements, markers)
  end

  defp split_at(opening, rest, _prefix, statement, _significant?, statements, markers)
       when opening in ["‘", "“"] do
    closing = if opening == "‘", do: "’", else: "”"
    rest = skip_through(rest, closing)
    split_statements(rest, statement, true, statements, markers)
  end

  defp split_at("$", rest, prefix, statement, _significant?, statements, markers) do
    # A dollar starts a heredoc only at a token boundary; otherwise it belongs to a bare word.
    rest =
      if Regex.match?(@bare_word_before_dollar, prefix) do
        skip_bare_word(rest)
      else
        case skip_heredoc(rest) do
          {:ok, rest} -> rest
          :error -> skip_bare_word(rest)
        end
      end

    split_statements(rest, statement, true, statements, markers)
  end

  defp skip_quoted(sql, quote) do
    pattern = :binary.compile_pattern([<<quote>>, "\\"])
    skip_quoted(sql, quote, pattern)
  end

  defp skip_quoted(sql, quote, pattern) do
    case :binary.match(sql, pattern) do
      :nomatch ->
        <<>>

      {offset, 1} ->
        <<_::binary-size(^offset), marker, rest::binary>> = sql

        case {marker, rest} do
          {?\\, <<_escaped, rest::binary>>} -> skip_quoted(rest, quote, pattern)
          {?\\, <<>>} -> <<>>
          {^quote, <<^quote, rest::binary>>} -> skip_quoted(rest, quote, pattern)
          {^quote, rest} -> rest
        end
    end
  end

  defp skip_block_comment(sql) do
    pattern = :binary.compile_pattern(["/*", "*/"])
    skip_block_comment(sql, 1, pattern)
  end

  defp skip_block_comment(sql, depth, pattern) do
    case :binary.match(sql, pattern) do
      :nomatch ->
        :error

      {offset, 2} ->
        <<_::binary-size(^offset), marker::binary-size(2), rest::binary>> = sql

        case marker do
          "/*" -> skip_block_comment(rest, depth + 1, pattern)
          "*/" when depth == 1 -> {:ok, rest}
          "*/" -> skip_block_comment(rest, depth - 1, pattern)
        end
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

  defp skip_bare_word(sql) do
    case Regex.run(@bare_word, sql, return: :index) do
      [{0, size}] -> binary_part(sql, size, byte_size(sql) - size)
      nil -> sql
    end
  end

  defp has_sql?(sql), do: String.trim(sql) != ""

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
