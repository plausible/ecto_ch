# measures the time required to build a simple select statement
# TODO add more statements, base them on Plausible code

import Ecto.Query

prepare = fn query ->
  {query, _params, _key} = Ecto.Query.Planner.plan(query, :all, Ecto.Adapters.ClickHouse)
  {query, _} = Ecto.Query.Planner.normalize(query, :all, Ecto.Adapters.ClickHouse, _counter = 0)
  query
end

simple = prepare.(select("events", [e], e.id))

Benchee.run(%{
  "simple" => fn -> Ecto.Adapters.ClickHouse.Connection.all(simple) end
})
