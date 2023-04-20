defmodule Ecto.Integration.GroupingTest do
  use Ecto.Integration.Case
  import Ecto.Query

  alias Ecto.Integration.TestRepo
  alias Ecto.Integration.Post

  test "Grouping on already known field" do
    TestRepo.insert!(%Post{title: "1", counter: 1, public: true})
    TestRepo.insert!(%Post{title: "2", counter: 2, public: true})
    TestRepo.insert!(%Post{title: "3", counter: 3, public: true})
    TestRepo.insert!(%Post{title: "4", counter: 4, public: true})

    data =
      TestRepo.all(
        from(
          p in Post,
          select: %{
            counter: fragment("modulo(?, 2) == 0", p.counter),
            total_counter: sum(p.counter)
          },
          group_by: fragment("counter")
        )
      )

    assert Enum.sort(data) ==
             Enum.sort([%{counter: 0, total_counter: 4}, %{counter: 1, total_counter: 6}])
  end

  test "Grouping on new selected field" do
    TestRepo.insert!(%Post{title: "1", counter: 1, public: true})
    TestRepo.insert!(%Post{title: "2", counter: 2, public: true})
    TestRepo.insert!(%Post{title: "3", counter: 3, public: true})
    TestRepo.insert!(%Post{title: "4", counter: 4, public: true})

    data =
      TestRepo.all(
        from(
          p in Post,
          select: %{
            even: fragment("modulo(?, 2) == 0", p.counter),
            total_counter: sum(p.counter)
          },
          group_by: fragment("even")
        )
      )

    assert Enum.sort(data) ==
             Enum.sort([%{even: 0, total_counter: 4}, %{even: 1, total_counter: 6}])
  end
end
