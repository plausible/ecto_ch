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
  def dumpers({:map, _subtype}, type), do: [&Ecto.Type.embedded_dump(type, &1, :json)]
  def dumpers({:in, subtype}, _type), do: [{:array, subtype}]
  def dumpers(:boolean, type), do: [type, &bool_encode/1]
  def dumpers(:binary_id, type), do: [type, Ecto.UUID]
  def dumpers(_primitive, type), do: [type]

  # TODO needed? can do in :ch?
  defp bool_encode(1), do: {:ok, true}
  defp bool_encode(0), do: {:ok, false}
  defp bool_encode(x), do: {:ok, x}

  @impl Ecto.Adapter
  def loaders({:map, _subtype}, type), do: [&Ecto.Type.embedded_load(type, &1, :json)]
  def loaders(:binary_id, type), do: [Ecto.UUID, type]
  def loaders(:boolean, type), do: [&bool_decode/1, type]
  def loaders(:float, type), do: [&float_decode/1, type]
  def loaders(_primitive, type), do: [type]

  defp bool_decode(1), do: {:ok, true}
  defp bool_decode(0), do: {:ok, false}
  defp bool_decode(x), do: {:ok, x}

  defp float_decode(%Decimal{} = decimal), do: {:ok, Decimal.to_float(decimal)}
  defp float_decode(x), do: {:ok, x}

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

    opts =
      case operation do
        :all -> put_setting(opts, :readonly, 1)
        _other -> opts
      end

    result = Ecto.Adapters.SQL.query!(adapter_meta, sql, params, opts)

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
end
