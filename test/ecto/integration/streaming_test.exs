defmodule Ecto.Integration.StreamingTest do
  use Ecto.Integration.Case

  alias Ecto.Integration.TestRepo
  alias EctoClickHouse.Integration.User

  import Ecto.Query

  test "streams are not supported" do
    {:ok, _} = TestRepo.insert(User.changeset(%User{}, %{name: "Bill"}))
    {:ok, _} = TestRepo.insert(User.changeset(%User{}, %{name: "Shannon"}))
    {:ok, _} = TestRepo.insert(User.changeset(%User{}, %{name: "Tom"}))
    {:ok, _} = TestRepo.insert(User.changeset(%User{}, %{name: "Tiffany"}))
    {:ok, _} = TestRepo.insert(User.changeset(%User{}, %{name: "Dave"}))

    assert_raise RuntimeError, "ClickHouse does not support cursors", fn ->
      TestRepo.transaction(fn ->
        User
        |> select([u], u)
        |> TestRepo.stream()
        |> Enum.map(fn user -> user end)
        |> Enum.count()
      end)
    end
  end
end
