defmodule Ecto.Integration.UnionTest do
  use Ecto.Integration.Case
  import Ecto.Query

  alias Ecto.Integration.TestRepo
  alias Ecto.Integration.Post

  test "union & ordering" do
    TestRepo.insert!(%Post{title: "hello", counter: 1, public: true})
    TestRepo.insert!(%Post{title: "hello", counter: 1, public: true})

    TestRepo.insert!(%Post{title: "bye", counter: 2, public: false})

    other =
      from(
        p in Post,
        where: p.public,
        group_by: p.public,
        order_by: fragment("total_counter"),
        select: %{
          public: p.public,
          total_counter: sum(p.counter)
        }
      )

    data =
      from(
        p in Post,
        union_all: ^other,
        where: not p.public,
        group_by: p.public,
        order_by: fragment("total_counter"),
        select: %{
          public: p.public,
          total_counter: sum(p.counter)
        }
      )
      |> TestRepo.all()

    assert data == [%{public: false, total_counter: 2}, %{public: true, total_counter: 2}]
  end
end
