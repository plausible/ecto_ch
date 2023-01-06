defmodule Ecto.Adapters.ClickHouse do
  @moduledoc """
  Ecto adapter for ClickHouse database
  """

  @behaviour Ecto.Adapter
  # @behaviour Ecto.Adapter.Migration
  @behaviour Ecto.Adapter.Queryable
  # @behaviour Ecto.Adapter.Schema
  # @behaviour Ecto.Adapter.Transaction
  @driver :ch
  @conn __MODULE__.Connection

  @impl Ecto.Adapter
  defmacro __before_compile__(env) do
    Ecto.Adapters.SQL.__before_compile__(@driver, env)
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
  def loaders({:map, _}, type), do: [&Ecto.Type.embedded_load(type, &1, :json)]
  def loaders(:binary_id, type), do: [Ecto.UUID, type]
  def loaders(_, type), do: [type]

  @impl Ecto.Adapter
  def dumpers({:map, _}, type), do: [&Ecto.Type.embedded_dump(type, &1, :json)]
  def dumpers(:binary_id, type), do: [type, Ecto.UUID]
  def dumpers(_, type), do: [type]

  ## Query

  @impl Ecto.Adapter.Queryable
  def prepare(:all, query) do
    IO.inspect(query, label: "prepare")
    {:nocache, @conn.all(query)}
  end

  # @impl Ecto.Adapter.Queryable
  # def execute(adapter_meta, query_meta, query, params, opts) do
  #   IO.inspect()
  #   Ecto.Adapters.SQL.execute(:named, adapter_meta, query_meta, query, params, opts)
  # end
end
