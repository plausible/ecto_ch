defmodule Dev do
  def start_repo do
    Repo.start_link()
  end

  def demo do
    Rexbug.start(
      [
        "Ecto.Query",
        "Ecto.Query.Builder",
        "Ecto.Query.Planner",
        "Ecto.Adapters.ClickHouse.Connection"
      ],
      msgs: 1000
    )

    result = Repo.all(query(_name = "John"))
    Rexbug.stop()
    result
  end

  import Ecto.Query

  def query(name) do
    "table"
    |> where([t], t.name == type(^name, :string))
    |> select([t], t.id)
  end
end
