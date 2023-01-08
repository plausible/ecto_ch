defmodule Ecto.Adapters.ClickHouse do
  use Ecto.Adapters.SQL, driver: :ch

  @impl Ecto.Adapter.Migration
  def supports_ddl_transaction?, do: false

  @impl Ecto.Adapter.Migration
  def lock_for_migrations(_meta, _options, f), do: f.()

  @impl Ecto.Adapter.Schema
  def autogenerate(:id), do: nil
  def autogenerate(:embed_id), do: Ecto.UUID.generate()
  def autogenerate(:binary_id), do: Ecto.UUID.generate()

  @impl Ecto.Adapter.Queryable
  def prepare(_operation, query) do
    {:nocache, query}
  end

  @impl Ecto.Adapter.Queryable
  def execute(adapter_meta, query_meta, {:nocache, query} = q, params, opts) do
    q = put_elem(q, 1, {_id = 0, @conn.all(query, params)})
    Ecto.Adapters.SQL.execute(:unnamed, adapter_meta, query_meta, q, params, opts)
  end
end
