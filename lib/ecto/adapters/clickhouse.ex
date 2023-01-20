defmodule Ecto.Adapters.ClickHouse do
  @moduledoc "TODO"
  # TODO fix warnings
  use Ecto.Adapters.SQL, driver: :ch
  @behaviour Ecto.Adapter.Storage
  @behaviour Ecto.Adapter.Structure
  @conn __MODULE__.Connection

  @impl Ecto.Adapter.Migration
  def supports_ddl_transaction?, do: false

  @impl Ecto.Adapter.Migration
  def lock_for_migrations(_meta, _options, f), do: f.()

  @impl Ecto.Adapter.Schema
  def autogenerate(:id), do: nil
  def autogenerate(:embed_id), do: Ecto.UUID.generate()
  def autogenerate(:binary_id), do: Ecto.UUID.generate()

  @impl Ecto.Adapter.Queryable
  def prepare(_operation, query), do: {:nocache, query}

  @impl Ecto.Adapter.Queryable
  def execute(adapter_meta, query_meta, {:nocache, query} = prepared, params, opts) do
    {sql, params} = @conn.all(query, params)
    prepared = put_elem(prepared, 1, {_id = 0, sql})
    Ecto.Adapters.SQL.execute(:unnamed, adapter_meta, query_meta, prepared, params, opts)
  end

  @impl Ecto.Adapter
  defmacro __before_compile__(_env) do
    [
      # can't use it directly because to_sql doesn't work with the prepare/execute hack above
      # Ecto.Adapters.SQL.__before_compile__(@driver, env),
      quote do
        @doc """
        A convenience function for SQL-based repositories that executes the given query.

        See `Ecto.Adapters.SQL.query/4` for more information.
        """
        def query(sql, params \\ [], opts \\ []) do
          Ecto.Adapters.SQL.query(get_dynamic_repo(), sql, params, opts)
        end

        @doc """
        A convenience function for SQL-based repositories that executes the given query.

        See `Ecto.Adapters.SQL.query!/4` for more information.
        """
        def query!(sql, params \\ [], opts \\ []) do
          Ecto.Adapters.SQL.query!(get_dynamic_repo(), sql, params, opts)
        end

        @doc """
        A convenience function for SQL-based repositories that executes the given multi-result query.

        See `Ecto.Adapters.SQL.query_many/4` for more information.
        """
        def query_many(sql, params \\ [], opts \\ []) do
          Ecto.Adapters.SQL.query_many(get_dynamic_repo(), sql, params, opts)
        end

        @doc """
        A convenience function for SQL-based repositories that executes the given multi-result query.

        See `Ecto.Adapters.SQL.query_many!/4` for more information.
        """
        def query_many!(sql, params \\ [], opts \\ []) do
          Ecto.Adapters.SQL.query_many!(get_dynamic_repo(), sql, params, opts)
        end

        @doc """
        A convenience function for SQL-based repositories that translates the given query to SQL.

        See `Ecto.Adapters.SQL.to_sql/3` for more information.
        """
        def to_sql(operation, queryable) do
          Ecto.Adapters.ClickHouse.to_sql(operation, get_dynamic_repo(), queryable)
        end

        @doc """
        A convenience function for SQL-based repositories that executes an EXPLAIN statement or similar
        depending on the adapter to obtain statistics for the given query.

        See `Ecto.Adapters.SQL.explain/4` for more information.
        """
        def explain(operation, queryable, opts \\ []) do
          Ecto.Adapters.SQL.explain(get_dynamic_repo(), operation, queryable, opts)
        end

        @doc """
        A convenience function for SQL-based repositories that forces all connections in the
        pool to disconnect within the given interval.

        See `Ecto.Adapters.SQL.disconnect_all/3` for more information.
        """
        def disconnect_all(interval, opts \\ []) do
          Ecto.Adapters.SQL.disconnect_all(get_dynamic_repo(), interval, opts)
        end
      end,
      quote do
        def insert_stream(table_or_schema, rows, opts \\ []) do
          Ecto.Adapters.ClickHouse.insert_stream(get_dynamic_repo(), table_or_schema, rows, opts)
        end
      end
    ]
  end

  @impl Ecto.Adapter.Schema
  def insert_all(
        adapter_meta,
        schema_meta,
        header,
        rows,
        _on_conflict,
        _returning,
        _placeholders,
        opts
      ) do
    %{source: source, prefix: prefix, schema: schema} = schema_meta

    types =
      if schema do
        extract_types(schema, header)
      else
        opts[:types] || raise "missing :types"
      end

    # TODO support queries like INSERT INTO ... SELECT FROM
    rows = unzip_insert(rows, header)
    sql = @conn.insert(prefix, source, header)
    opts = [{:types, types}, {:command, :insert} | opts]

    %{num_rows: num, rows: rows} = Ecto.Adapters.SQL.query!(adapter_meta, sql, rows, opts)
    {num, rows}
  end

  @impl Ecto.Adapter.Storage
  def storage_up(opts) do
    alias Ch.{Query, Error}
    alias Ch.Connection, as: Conn

    # TODO use Identifier later
    {database, opts} = Keyword.pop!(opts, :database)
    query = Query.build("CREATE DATABASE #{@conn.quote_name(database)}", command: :create)

    with {:ok, conn} <- Conn.connect(opts),
         {:ok, _query, _result, _conn} <- Conn.handle_execute(query, [], [], conn) do
      :ok
    else
      {:disconnect, reason, _conn} -> {:error, reason}
      {:error, %Error{code: 82}, _conn} -> {:error, :already_up}
      {:error, reason, _conn} -> {:error, reason}
      {:error, _reason} = error -> error
    end
  end

  @impl Ecto.Adapter.Storage
  def storage_down(opts) do
    alias Ch.{Query, Error}
    alias Ch.Connection, as: Conn

    # TODO use Identifier later
    {database, opts} = Keyword.pop!(opts, :database)
    query = Query.build("DROP DATABASE #{@conn.quote_name(database)}", command: :drop)

    with {:ok, conn} <- Conn.connect(opts),
         {:ok, _query, _result, _conn} <- Conn.handle_execute(query, [], [], conn) do
      :ok
    else
      {:disconnect, reason, _conn} -> {:error, reason}
      {:error, %Error{code: 81}, _conn} -> {:error, :already_down}
      {:error, reason, _conn} -> {:error, reason}
      {:error, _reason} = error -> error
    end
  end

  @impl Ecto.Adapter.Storage
  def storage_status(opts) do
    alias Ch.Query
    alias Ch.Connection, as: Conn

    {database, opts} = Keyword.pop!(opts, :database)
    statement = "SELECT 1 FROM system.databases WHERE name = {database:String}"
    params = %{"database" => database}
    query = Query.build(statement, command: :select)

    with {:ok, conn} <- Conn.connect(opts),
         {:ok, _query, %{num_rows: num_rows}, _conn} <-
           Conn.handle_execute(query, params, [], conn) do
      case num_rows do
        1 -> :up
        0 -> :down
      end
    else
      {:disconnect, reason, _conn} -> {:error, reason}
      {:error, reason, _conn} -> {:error, reason}
      {:error, _reason} = error -> error
    end
  end

  @impl Ecto.Adapter.Structure
  def structure_dump(default, config) do
    alias Ch.Query
    alias Ch.Connection, as: Conn

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
    alias Ch.Query
    alias Ch.Connection, as: Conn

    show_tables_q = Query.build("SHOW TABLES", command: :show)

    case Conn.handle_execute(show_tables_q, [], [], conn) do
      {:ok, _query, %{rows: rows}, conn} ->
        tables = Enum.map(rows, fn [table] -> table end)
        {:ok, _schema, _conn} = structure_dump_tables(conn, tables)

      {:disconnect, reason, _conn} ->
        {:error, reason}

      {:error, reason, _conn} ->
        {:error, reason}
    end
  end

  defp structure_dump_tables(conn, tables) do
    alias Ch.Query
    alias Ch.Connection, as: Conn

    # TODO use Identifier later
    query = fn table ->
      Query.build("SHOW CREATE TABLE #{@conn.quote_name(table)}", command: :show)
    end

    result =
      Enum.reduce_while(tables, {[], conn}, fn table, {schemas, conn} ->
        case Conn.handle_execute(query.(table), [], [], conn) do
          {:ok, _query, %{rows: [[schema]]}, conn} -> {:cont, {[schema, ?\n | schemas], conn}}
          {:error, reason, _conn} -> {:halt, {:error, reason}}
          {:disconnect, reason, _conn} -> {:halt, {:error, reason}}
        end
      end)

    case result do
      {:error, _reason} = error -> error
      success -> {:ok, success, conn}
    end
  end

  defp structure_dump_versions(conn, table) do
    alias Ch.Query
    alias Ch.Connection, as: Conn

    table = @conn.quote_name(table)

    # TODO use Identifier later
    query = Query.build("SELECT * FROM #{table} FORMAT CSVWithNames", command: :select)

    case Conn.handle_execute(query, [], [format: "VALUES"], conn) do
      {:ok, _query, %{rows: rows}, conn} ->
        versions = ["INSERT INTO ", table, "(version, inserted_at) VALUES " | rows]
        {:ok, versions, conn}

      {:error, reason, _conn} ->
        {:error, reason}

      {:disconnect, reason, _conn} ->
        {:error, reason}
    end
  end

  @impl Ecto.Adapter.Structure
  def dump_cmd(_args, _opts, _config) do
    raise "not implemented"
  end

  @impl Ecto.Adapter.Structure
  def structure_load(_default, _config) do
    raise "not implemented"
  end

  # TODO support queries and placeholders, benchmark
  defp unzip_insert([row | rows], header) do
    [unzip_row(header, row) | unzip_insert(rows, header)]
  end

  defp unzip_insert([], _header), do: []

  defp unzip_row([field | fields], row) do
    case :lists.keyfind(field, 1, row) do
      {_, value} -> [value | unzip_row(fields, row)]
      false -> [nil | unzip_row(fields, row)]
    end
  end

  defp unzip_row([], _row), do: []

  @impl Ecto.Adapter.Schema
  def insert(adapter_meta, schema_meta, params, _on_conflict, _returning, opts) do
    %{source: source, prefix: prefix, schema: schema} = schema_meta
    {header, row} = Enum.unzip(params)

    types =
      if schema do
        extract_types(schema, header)
      else
        opts[:types] || raise "missing :types"
      end

    sql = @conn.insert(prefix, source, header)
    opts = [{:types, types}, {:command, :insert} | opts]

    case Ecto.Adapters.SQL.query!(adapter_meta, sql, [row], opts) do
      %{num_rows: 1} ->
        {:ok, []}

      # TODO workaround for v21.11 not retrning written_rows in summary
      %{num_rows: 0} ->
        # if on_conflict == :nothing, do: {:ok, []}, else: {:error, :stale}
        {:ok, []}
    end
  end

  @impl Ecto.Adapter.Schema
  def update(_adapter_meta, _schema_meta, _fields, _params, _returning, _opts) do
    raise "not implemented"
  end

  @impl Ecto.Adapter.Schema
  def delete(_adapter_meta, _schema_meta, _params, _opts) do
    raise "not implemented"
  end

  def insert_stream(repo, table, rows, opts) do
    {statement, opts} = build_insert(table, opts)
    opts = put_in(opts, [:command], :insert)

    with {:ok, %{num_rows: num_rows}} <- Ecto.Adapters.SQL.query(repo, statement, rows, opts) do
      {:ok, num_rows}
    end
  end

  @doc false
  def build_insert(table, opts) when is_binary(table) do
    statement = build_insert_statement(opts[:prefix], table, opts[:fields])
    {statement, opts}
  end

  def build_insert(schema, opts) when is_atom(schema) do
    prefix = schema.__schema__(:prefix)
    table = schema.__schema__(:source)
    fields = schema.__schema__(:fields)
    types = extract_types(schema, fields)
    statement = build_insert_statement(prefix, table, fields)
    opts = put_in(opts, [:types], types)
    {statement, opts}
  end

  defp build_insert_statement(prefix, table, fields) do
    fields =
      case fields do
        [_ | _] = fields -> [?(, @conn.intersperse_map(fields, ?,, &@conn.quote_name/1), ?)]
        _none -> []
      end

    ["INSERT INTO ", @conn.quote_table(prefix, table) | fields]
  end

  defp extract_types(schema, fields) do
    Enum.map(fields, fn field ->
      :type |> schema.__schema__(field) |> Ecto.Type.type() |> remap_type()
    end)
  end

  # TODO
  defp remap_type(:naive_datetime), do: :datetime
  defp remap_type(:integer), do: :i64
  defp remap_type(other), do: other

  def to_sql(:all = kind, repo, queryable) do
    {{:nocache, query}, params} = Ecto.Adapter.Queryable.prepare_query(kind, repo, queryable)
    {sql, _params} = result = @conn.all(query, params)
    put_elem(result, 0, IO.iodata_to_binary(sql))
  end
end
