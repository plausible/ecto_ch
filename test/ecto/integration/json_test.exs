defmodule Ecto.Integration.JsonTest do
  use Ecto.Integration.Case

  @moduletag :json

  alias Ecto.Integration.TestRepo
  alias EctoClickHouse.Integration.Setting

  @tag :skip
  test "serializes json correctly" do
    # Insert a record purposefully with atoms as the map key. We are going to
    # verify later they were coerced into strings.
    setting =
      %Setting{}
      |> Setting.changeset(%{properties: %{foo: "bar", qux: "baz"}})
      |> TestRepo.insert!()

    # Read the record back using ecto and confirm it
    assert %Setting{properties: %{"foo" => "bar", "qux" => "baz"}} =
             TestRepo.get(Setting, setting.id)

    assert %{num_rows: 1, rows: [["bar"]]} =
             TestRepo.query!(
               "select json_extract(properties, '$.foo') from settings where id = ?1",
               [setting.id]
             )
  end

  defmodule SemiStructured do
    use Ecto.Schema

    @primary_key false
    schema "semi_structured" do
      field :json, Ch, type: "JSON"
      field :time, :naive_datetime
    end
  end

  test "it works" do
    TestRepo.query!("""
    CREATE TABLE semi_structured (
      json JSON,
      time DateTime
    ) ENGINE MergeTree ORDER BY time
    """)

    TestRepo.insert!(%SemiStructured{json: %{foo: "bar", baz: 42}, time: ~N[2023-10-01 12:00:00]})

    assert [
             %SemiStructured{
               json: %{"foo" => "bar", "baz" => "42"},
               time: ~N[2023-10-01 12:00:00]
             }
           ] =
             TestRepo.all(SemiStructured)
  end
end
