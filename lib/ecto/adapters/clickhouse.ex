defmodule Ecto.Adapters.ClickHouse do
  @moduledoc "Ecto adapter for a minimal HTTP ClickHouse client"

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Migration
  @behaviour Ecto.Adapter.Queryable
  @behaviour Ecto.Adapter.Schema
  @behaviour Ecto.Adapter.Storage
  @behaviour Ecto.Adapter.Structure

  @conn __MODULE__.Connection
  @driver :ch

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
      A convenience function for SQL-based repositories that translates the given query to SQL.

      See `Ecto.Adapters.SQL.to_sql/3` for more information.
      """
      def to_sql(operation, queryable) do
        Ecto.Adapters.ClickHouse.to_sql(operation, queryable)
      end

      @doc """
      A convenience function for SQL-based repositories that forces all connections in the
      pool to disconnect within the given interval.

      See `Ecto.Adapters.SQL.disconnect_all/3` for more information.
      """
      def disconnect_all(interval, opts \\ []) do
        Ecto.Adapters.SQL.disconnect_all(get_dynamic_repo(), interval, opts)
      end
    end
  end

  @impl Ecto.Adapter
  def ensure_all_started(config, type) do
    Ecto.Adapters.SQL.ensure_all_started(@driver, config, type)
  end

  @impl Ecto.Adapter
  def init(config) do
    Ecto.Adapters.SQL.init(@conn, @driver, config)
  end

  @impl Ecto.Adapter
  def checkout(meta, opts, fun) do
    Ecto.Adapters.SQL.checkout(meta, opts, fun)
  end

  @impl Ecto.Adapter
  def checked_out?(meta) do
    Ecto.Adapters.SQL.checked_out?(meta)
  end

  @impl Ecto.Adapter
  def dumpers({:in, subtype}, _type), do: [{:array, subtype}]
  def dumpers(:boolean, type), do: [type, &bool_encode/1]
  def dumpers(:uuid, Ecto.UUID), do: [&uuid_encode/1]
  def dumpers(:uuid, type), do: [type, &uuid_encode/1]
  def dumpers(_primitive, type), do: [type]

  defp bool_encode(1), do: {:ok, true}
  defp bool_encode(0), do: {:ok, false}
  defp bool_encode(x), do: {:ok, x}

  defp uuid_encode(uuid), do: Ecto.UUID.cast(uuid)

  @impl Ecto.Adapter
  def loaders(:binary_id, type), do: [Ecto.UUID, type]
  def loaders(:boolean, type), do: [&bool_decode/1, type]
  def loaders(:float, type), do: [&float_decode/1, type]
  def loaders(_primitive, type), do: [type]

  defp bool_decode(1), do: {:ok, true}
  defp bool_decode(0), do: {:ok, false}

  defp float_decode(%Decimal{} = decimal), do: {:ok, Decimal.to_float(decimal)}

  @impl Ecto.Adapter.Migration
  def supports_ddl_transaction?, do: false

  @impl Ecto.Adapter.Migration
  def lock_for_migrations(_meta, _options, f), do: f.()

  @impl Ecto.Adapter.Migration
  def execute_ddl(meta, definition, opts) do
    Ecto.Adapters.SQL.execute_ddl(meta, @conn, definition, opts)
  end

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
  def autogenerate(:id), do: nil
  def autogenerate(:embed_id), do: Ecto.UUID.generate()
  def autogenerate(:binary_id), do: Ecto.UUID.bingenerate()

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

  @dialyzer {:no_return, update: 6}
  @impl Ecto.Adapter.Schema
  def update(adapter_meta, %{source: source, prefix: prefix}, fields, params, returning, opts) do
    {fields, field_values} = :lists.unzip(fields)
    filter_values = Keyword.values(params)
    sql = @conn.update(prefix, source, fields, params, returning)

    Ecto.Adapters.SQL.struct(
      adapter_meta,
      @conn,
      sql,
      :update,
      source,
      params,
      field_values ++ filter_values,
      :raise,
      returning,
      opts
    )
  end

  @impl Ecto.Adapter.Schema
  def delete(adapter_meta, schema_meta, params, opts) do
    Ecto.Adapters.ClickHouse.Schema.delete(adapter_meta, schema_meta, params, opts)
  end

  @impl Ecto.Adapter.Queryable
  def stream(adapter_meta, query_meta, query, params, opts) do
    Ecto.Adapters.SQL.stream(adapter_meta, query_meta, query, params, opts)
  end

  @impl Ecto.Adapter.Queryable
  def prepare(operation, query), do: {:nocache, {operation, query}}

  @impl Ecto.Adapter.Queryable
  def execute(adapter_meta, query_meta, {:nocache, {operation, query}}, params, opts) do
    sql = prepare_sql(operation, query, params)

    opts =
      case operation do
        :all ->
          [{:command, :select} | put_setting(opts, :readonly, 1)]

        :delete_all ->
          [{:command, :delete} | opts]
      end

    result = Ecto.Adapters.SQL.query!(adapter_meta, sql, params, put_source(opts, query_meta))

    case operation do
      :all ->
        %{num_rows: num_rows, rows: rows} = result
        {num_rows, rows}

      :delete_all ->
        # clickhouse doesn't give us any info on how many have been deleted
        {0, nil}
    end
  end

  @doc false
  def to_sql(operation, queryable) do
    queryable =
      queryable
      |> Ecto.Queryable.to_query()
      |> Ecto.Query.Planner.ensure_select(operation == :all)

    {query, _cast_params, dump_params} =
      Ecto.Adapter.Queryable.plan_query(operation, Ecto.Adapters.ClickHouse, queryable)

    sql = Ecto.Adapters.ClickHouse.prepare_sql(operation, query, dump_params)
    {IO.iodata_to_binary(sql), dump_params}
  end

  defp put_setting(opts, key, value) do
    setting = {key, value}
    Keyword.update(opts, :settings, [setting], fn settings -> [setting | settings] end)
  end

  @doc false
  def prepare_sql(:all, query, params), do: @conn.all(query, params)
  def prepare_sql(:update_all, query, params), do: @conn.update_all(query, params)
  def prepare_sql(:delete_all, query, params), do: @conn.delete_all(query, params)

  defp put_source(opts, %{sources: sources}) when is_binary(elem(elem(sources, 0), 0)) do
    {source, _, _} = elem(sources, 0)
    [source: source] ++ opts
  end

  defp put_source(opts, _) do
    opts
  end
end
