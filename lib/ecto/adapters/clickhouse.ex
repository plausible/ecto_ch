defmodule Ecto.Adapters.ClickHouse do
  @moduledoc "Ecto adapter for a minimal HTTP ClickHouse client"
  use Ecto.Adapters.SQL, driver: :ch

  @dialyzer {:no_return, update: 6, rollback: 2}

  @behaviour Ecto.Adapter.Storage
  @behaviour Ecto.Adapter.Structure

  @impl Ecto.Adapter
  defmacro __before_compile__(_env) do
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
        {query, params} = Ecto.Adapters.SQL.to_sql(operation, get_dynamic_repo(), queryable)
        sql = Ecto.Adapters.ClickHouse.prepare_sql(operation, query, params)
        {IO.iodata_to_binary(sql), params}
      end

      @doc """
      A convenience function for SQL-based repositories that forces all connections in the
      pool to disconnect within the given interval.

      See `Ecto.Adapters.SQL.disconnect_all/3` for more information.
      """
      def disconnect_all(interval, opts \\ []) do
        Ecto.Adapters.SQL.disconnect_all(get_dynamic_repo(), interval, opts)
      end

      # TODO
      # def insert_stream(schema_or_source, stream, opts \\ []) do
      #   repo = get_dynamic_repo()
      #   tuplet = Ecto.Repo.Supervisor.tuplet(repo, prepare_opts(:insert_stream, opts))
      #   Ecto.Adapters.ClickHouse.Schema.insert_stream(schema_or_source, stream, tuplet)
      # end

      # TODO alter_delete_all, alter_update_all, explain
    end
  end

  # TODO loaders for bool?

  @impl Ecto.Adapter
  def dumpers({:map, _subtype}, type), do: [&Ecto.Type.embedded_dump(type, &1, :json)]
  def dumpers({:in, subtype}, _type), do: [{:array, subtype}]
  def dumpers(:binary_id, type), do: [type, Ecto.UUID]
  def dumpers(_primitive, type), do: [type]

  @impl Ecto.Adapter.Migration
  def supports_ddl_transaction?, do: false

  @impl Ecto.Adapter.Migration
  def lock_for_migrations(_meta, _options, f), do: f.()

  @impl Ecto.Adapter.Storage
  defdelegate storage_up(opts), to: Ecto.Adapters.ClickHouse.Storage

  @impl Ecto.Adapter.Storage
  defdelegate storage_down(opts), to: Ecto.Adapters.ClickHouse.Storage

  @impl Ecto.Adapter.Storage
  defdelegate storage_status(opts), to: Ecto.Adapters.ClickHouse.Storage

  @impl Ecto.Adapter.Structure
  defdelegate structure_dump(default, config), to: Ecto.Adapters.ClickHouse.Structure

  @impl Ecto.Adapter.Structure
  def structure_load(_default, _config) do
    raise "not implemented"
  end

  @impl Ecto.Adapter.Structure
  def dump_cmd(_args, _opts, _config) do
    raise "not implemented"
  end

  @impl Ecto.Adapter.Schema
  def insert_all(
        adapter_meta,
        schema_meta,
        header,
        rows,
        on_conflict,
        returning,
        placeholders,
        opts
      ) do
    Ecto.Adapters.ClickHouse.Schema.insert_all(
      adapter_meta,
      schema_meta,
      header,
      rows,
      on_conflict,
      returning,
      placeholders,
      opts
    )
  end

  @impl Ecto.Adapter.Schema
  def insert(adapter_meta, schema_meta, params, _, _, opts) do
    Ecto.Adapters.ClickHouse.Schema.insert(adapter_meta, schema_meta, params, opts)
  end

  @impl Ecto.Adapter.Schema
  def delete(adapter_meta, schema_meta, params, opts) do
    Ecto.Adapters.ClickHouse.Schema.delete(adapter_meta, schema_meta, params, opts)
  end

  @impl Ecto.Adapter.Queryable
  def prepare(operation, query), do: {:nocache, {operation, query}}

  @impl Ecto.Adapter.Queryable
  def execute(adapter_meta, _query_meta, {:nocache, {operation, query}}, params, opts) do
    sql = prepare_sql(operation, query, params)
    result = Ecto.Adapters.SQL.query!(adapter_meta, sql, params, opts)

    case operation do
      :all ->
        # TODO formats?
        %{num_rows: num_rows, rows: rows} = result
        {num_rows, rows}

      :delete_all ->
        # TODO
        {1, nil}
    end
  end

  @doc false
  def prepare_sql(:all, query, params), do: @conn.all(query, params)
  def prepare_sql(:update_all, query, params), do: @conn.update_all(query, params)
  def prepare_sql(:delete_all, query, params), do: @conn.delete_all(query, params)
end
