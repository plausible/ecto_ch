defmodule Ecto.Integration.JsonTest do
  use Ecto.Integration.Case
  import Ecto.Query, only: [from: 2]

  @moduletag :json

  alias Ecto.Integration.TestRepo
  alias EctoClickHouse.Integration.Setting

  @tag skip: true
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

  test "basic" do
    TestRepo.query!("""
    CREATE TABLE semi_structured (
      json JSON,
      time DateTime
    ) ENGINE MergeTree ORDER BY time
    """)

    on_exit(fn -> TestRepo.query!("DROP TABLE semi_structured") end)

    %SemiStructured{}
    |> Ecto.Changeset.cast(
      %{
        json: %{"from" => "insert"},
        time: ~N[2023-10-01 12:00:00]
      },
      [:json, :time]
    )
    |> TestRepo.insert!()

    TestRepo.insert_all(SemiStructured, [
      %{json: %{"from" => "insert_all"}, time: ~N[2023-10-01 13:00:00]},
      %{json: %{"from" => "another_insert_all"}, time: ~N[2023-10-01 13:01:00]}
    ])

    assert TestRepo.all(from s in SemiStructured, select: s.json, order_by: s.time) == [
             %{"from" => "insert"},
             %{"from" => "insert_all"},
             %{"from" => "another_insert_all"}
           ]
  end

  # https://github.com/plausible/ecto_ch/pull/233#issuecomment-3079317842

  defmodule TokenInfoSchema do
    @moduledoc false
    use Ecto.Schema

    @primary_key false
    schema "token_infos" do
      field :mint, Ch, type: "String"
      field :data, Ch, type: "JSON", source: :data
      field :created_at, Ch, type: "DateTime"
    end
  end

  test "token_info_schema" do
    TestRepo.query!("""
    create table token_infos(
      mint String,
      data JSON,
      created_at DateTime
    ) engine = MergeTree order by created_at
    """)

    on_exit(fn -> TestRepo.query!("DROP TABLE token_infos") end)

    missing_tokens = [
      %{
        mint: "123",
        data: %{"name" => "Test", "nested" => %{"name" => "Test", "arr" => ["abc", "b=deb"]}},
        created_at: NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)
      },
      %{
        mint: "325",
        data: %{"name" => "Test", "nested" => %{"name" => "Test", "arr" => ["abc", "b=deb"]}},
        created_at: NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)
      }
    ]

    assert {2, nil} = TestRepo.insert_all(TokenInfoSchema, missing_tokens)

    assert TestRepo.all(
             from t in TokenInfoSchema,
               order_by: t.created_at,
               select: %{
                 mint: t.mint,
                 name: fragment("?.nested.name::text", t.data)
               }
           ) == [
             %{mint: "123", name: "Test"},
             %{mint: "325", name: "Test"}
           ]
  end
end
