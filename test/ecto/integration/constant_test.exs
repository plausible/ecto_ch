defmodule Ecto.Integration.ConstantTest do
  use Ecto.Integration.Case, async: true
  import Ecto.Query
  alias Ecto.Integration.TestRepo

  test "it works" do
    query =
      from n in "numbers",
        select: n.number,
        limit: fragment("?", constant(^3))

    assert TestRepo.all(query, database: "system") ==
             [0, 1, 2]

    query =
      from n in "one",
        select: fragment("?", constant(^"let's escape"))

    assert TestRepo.all(query, database: "system") == ["let's escape"]
  end
end
