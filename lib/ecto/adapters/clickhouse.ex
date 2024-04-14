defmodule Ecto.Adapters.ClickHouse do
  @moduledoc """
  Adapter module for ClickHouse.

  It uses `Ch` for communicating to the database.

  ## Options

  All options can be given via the repository
  configuration:

      config :your_app, YourApp.Repo,
        ...

    * `:hostname` - Server hostname (default: `"localhost"`)
    * `:username` - Username
    * `:password` - User password
    * `:port` - HTTP Server port (default: `8123`)
    * `:scheme` - HTTP scheme (default: `"http"`)
    * `:database` - the database to connect to (default: `"default"`)
    * `:settings` - Keyword list of connection settings
    * `:transport_opts` - Options to be given to the transport being used. See `Mint.HTTP1.connect/4` for more info

  """

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
      Similar to `to_sql/2` but inlines the parameters into the SQL query.

      See `Ecto.Adapters.ClickHouse.to_inline_sql/2` for more information.
      """
      @spec to_inline_sql(:all | :delete_all | :update_all, Ecto.Queryable.t()) :: String.t()
      def to_inline_sql(operation, queryable) do
        Ecto.Adapters.ClickHouse.to_inline_sql(operation, queryable)
      end

      @doc """
      A convenience function for SQL-based repositories that forces all connections in the
      pool to disconnect within the given interval.

      See `Ecto.Adapters.SQL.disconnect_all/3` for more information.
      """
      def disconnect_all(interval, opts \\ []) do
        Ecto.Adapters.SQL.disconnect_all(get_dynamic_repo(), interval, opts)
      end

      @doc """
      Similar to `insert_all/2` but with the following differences:

        - accepts rows as streams or lists
        - sends rows as a chunked request
        - doesn't autogenerate ids or does any other preprocessing

      Example:

          Repo.query!("create table ecto_ch_demo(a UInt64, b String) engine Null")

          defmodule Demo do
            use Ecto.Schema

            @primary_key false
            schema "ecto_ch_demo" do
              field :a, Ch, type: "UInt64"
              field :b, :string
            end
          end

          rows = Stream.map(1..100_000, fn i -> %{a: i, b: to_string(i)} end)
          {100_000, nil} = Repo.insert_stream(Demo, rows)

          # schemaless
          {100_000, nil} = Repo.insert_stream("ecto_ch_demo", rows, types: [a: Ch.Types.u64(), b: :string])

      """
      def insert_stream(source_or_schema, rows, opts \\ []) do
        repo = get_dynamic_repo()
        # TODO need it?
        # opts = Ecto.Repo.Supervisor.tuplet(repo, prepare_opts(:insert_all, opts))
        Ecto.Adapters.ClickHouse.Schema.insert_stream(repo, source_or_schema, rows, opts)
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
  def dumpers(:uuid, Ecto.UUID), do: [&__MODULE__.hex_uuid/1]
  def dumpers(:uuid, type), do: [type, &__MODULE__.hex_uuid/1]
  def dumpers(:binary_id, type), do: [type, &__MODULE__.hex_uuid/1]
  def dumpers(_primitive, {:parameterized, Ch, params}), do: [dumper(params)]
  def dumpers({:parameterized, Ch, params}, type), do: [type, dumper(params)]
  def dumpers(_primitive, type), do: [type]

  defp dumper(:uuid), do: &__MODULE__.hex_uuid/1
  defp dumper(:date32), do: :date
  defp dumper(:datetime), do: :naive_datetime
  defp dumper({:datetime, "UTC"}), do: :utc_datetime
  defp dumper({:datetime64, _precision}), do: :naive_datetime_usec
  defp dumper({:datetime64, _precision, "UTC"}), do: :utc_datetime_usec
  defp dumper({:nullable, type}), do: dumper(type)
  defp dumper({:low_cardinality, type}), do: dumper(type)
  defp dumper({:decimal = d, _precision, _scale}), do: d

  for size <- [32, 64, 128, 256] do
    defp dumper({unquote(:"decimal#{size}"), _scale}), do: :decimal
  end

  for size <- [8, 16, 32, 64, 128, 256] do
    defp dumper(unquote(:"i#{size}")), do: :integer
    defp dumper(unquote(:"u#{size}")), do: :integer
  end

  for size <- [32, 64] do
    defp dumper(unquote(:"f#{size}")), do: :float
  end

  defp dumper({:simple_aggregate_function, _name, type}), do: dumper(type)
  defp dumper({:array = array, type}), do: {array, dumper(type)}
  defp dumper(_type), do: &__MODULE__.ok_identity/1

  @impl Ecto.Adapter
  def loaders(:uuid, Ecto.UUID = uuid), do: [uuid]
  def loaders(:uuid, type), do: [Ecto.UUID, type]
  def loaders(:binary_id, type), do: [Ecto.UUID, type]
  def loaders(_primitive, {:parameterized, Ch, params}), do: [loader(params)]
  def loaders({:parameterized, Ch, params}, type), do: [loader(params), type]
  def loaders(_primitive, type), do: [type]

  defp loader(:uuid), do: Ecto.UUID
  defp loader({:nullable, type}), do: loader(type)
  defp loader({:low_cardinality, type}), do: loader(type)
  defp loader({:simple_aggregate_function, _name, type}), do: loader(type)
  defp loader({:array = array, type}), do: {array, loader(type)}
  defp loader(_type), do: &__MODULE__.ok_identity/1

  @doc false
  def ok_identity(value), do: {:ok, value}

  @doc false
  def hex_uuid(nil), do: {:ok, nil}
  def hex_uuid(value), do: Ecto.UUID.cast(value)

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
  defdelegate structure_load(default, config), to: Ecto.Adapters.ClickHouse.Structure

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

  # TODO
  # https://github.com/elixir-ecto/ecto/blob/master/CHANGELOG.md#v3110-2023-11-14
  # https://github.com/elixir-ecto/ecto/pull/4277
  # @impl Ecto.Adapter.Schema
  def delete(adapter_meta, schema_meta, params, opts) do
    Ecto.Adapters.ClickHouse.Schema.delete(adapter_meta, schema_meta, params, opts)
  end

  @impl Ecto.Adapter.Schema
  def delete(adapter_meta, schema_meta, params, _returning, opts) do
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
        # clickhouse doesn't give us any info on how many rows have been deleted
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

  @doc """
  TODO
  """
  @spec to_inline_sql(:all | :delete_all | :update_all, Ecto.Queryable.t()) :: String.t()
  def to_inline_sql(operation, queryable) do
    queryable =
      queryable
      |> Ecto.Queryable.to_query()
      |> Ecto.Query.Planner.ensure_select(operation == :all)

    {query, _cast_params, dump_params} =
      Ecto.Adapter.Queryable.plan_query(operation, Ecto.Adapters.ClickHouse, queryable)

    inline_params = Enum.map(dump_params, &@conn.mark_inline/1)
    sql = Ecto.Adapters.ClickHouse.prepare_sql(operation, query, inline_params)
    IO.iodata_to_binary(sql)
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
