# measures the time required to build a simple select statement
# TODO add more statements, base them on Plausible code

import Ecto.Query

prepare = fn query ->
  {query, params, _key} = Ecto.Query.Planner.plan(query, :all, Ecto.Adapters.ClickHouse)
  {params, _} = Enum.unzip(params)
  {query, _} = Ecto.Query.Planner.normalize(query, :all, Ecto.Adapters.ClickHouse, _counter = 0)
  {query, params}
end

events = select("events", [e], e.id)

posts =
  "posts"
  |> where(title: ^"hello")
  |> select([p], p.id)

comments =
  "comments"
  |> where([c], c.post_id in subquery(posts))
  |> select([c], c.x)

Benchee.run(
  %{
    "all" => fn {query, params} -> Ecto.Adapters.ClickHouse.Connection.all(query, params) end
  },
  memory_time: 2,
  inputs: %{
    "events" => prepare.(events),
    "posts" => prepare.(posts),
    "comments" => prepare.(comments)
  }
)
