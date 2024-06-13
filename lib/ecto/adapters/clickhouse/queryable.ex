defmodule Ecto.Adapters.ClickHouse.Queryable do
  @moduledoc false
  require Ecto.Query

  @doc false
  def alter_update_all(name, queryable, [], tuplet) do
    alter_update_all(name, queryable, tuplet)
  end

  def alter_update_all(name, queryable, updates, tuplet) do
    query = Ecto.Query.from(queryable, update: ^updates)
    alter_update_all(name, query, tuplet)
  end

  defp alter_update_all(name, queryable, tuplet) do
    query = Ecto.Queryable.to_query(queryable)
    execute(:alter_update_all, name, query, tuplet)
  end

  defp execute(:alter_update_all = operation, _name, query, {adapter_meta, opts}) do
    %{adapter: adapter, cache: cache, repo: repo} = adapter_meta

    {query, opts} = repo.prepare_query(operation, query, opts)
    query = Ecto.Query.Planner.attach_prefix(query, opts)

    {query_meta, {:nocache, {:update_all, query}}, cast_params, dump_params} =
      Ecto.Query.Planner.query(query, :update_all, cache, adapter, 0)

    %{select: nil} = query_meta
    opts = [cast_params: cast_params] ++ opts

    adapter.execute(
      adapter_meta,
      query_meta,
      {:nocache, {operation, query}},
      dump_params,
      opts
    )
  end
end
