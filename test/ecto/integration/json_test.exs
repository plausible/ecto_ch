defmodule Ecto.Integration.JsonTest do
  use Ecto.Integration.Case
  import Ecto.Query

  @moduletag :json

  alias Ecto.Integration.TestRepo
  alias EctoClickHouse.Integration.Setting

  test "serializes json correctly" do
    TestRepo.query!("""
    CREATE TABLE settings (
      properties JSON
    ) ENGINE MergeTree ORDER BY tuple()
    """)

    on_exit(fn -> TestRepo.query!("DROP TABLE settings") end)

    # Insert a record purposefully with atoms as the map key. We are going to
    # verify later they were coerced into strings.
    %Setting{}
    |> Setting.changeset(%{properties: %{foo: "bar", qux: "baz"}})
    |> TestRepo.insert!()

    # Read the record back using ecto and confirm it
    assert %Setting{properties: %{"foo" => "bar", "qux" => "baz"}} =
             TestRepo.one!(Setting)

    assert %{num_rows: 1, rows: [["bar"]]} =
             TestRepo.query!(
               "select properties.foo from settings",
               []
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

  test "json_extract_path uses native JSON paths" do
    TestRepo.query!("""
    CREATE TABLE semi_structured (
      json JSON,
      time DateTime
    ) ENGINE MergeTree ORDER BY time
    """)

    on_exit(fn -> TestRepo.query!("DROP TABLE semi_structured") end)

    TestRepo.insert_all(SemiStructured, [
      %{
        json: %{
          "from" => "insert_all",
          "nested" => %{"name" => "Test", "arr" => ["abc", "b=deb"]},
          "not an identifier" => %{"a`b" => "escaped"}
        },
        time: ~N[2023-10-01 13:00:00]
      }
    ])

    assert TestRepo.all(
             from s in SemiStructured,
               select: %{
                 from: json_extract_path(s.json, ["from"]),
                 name: json_extract_path(s.json, ["nested", "name"]),
                 escaped: json_extract_path(s.json, ["not an identifier", "a`b"]),
                 bracket_name: s.json["nested"]["name"]
               }
           ) == [
             %{from: "insert_all", name: "Test", escaped: "escaped", bracket_name: "Test"}
           ]
  end

  # https://github.com/plausible/ecto_ch/pull/233#issuecomment-3079317842

  defmodule TokenInfoSchema do
    @moduledoc false
    use Ecto.Schema

    @primary_key false
    schema "token_infos" do
      field :mint, :string
      field :data, Ch, type: "JSON"
      field :created_at, :naive_datetime
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
               order_by: t.mint,
               select: %{
                 mint: t.mint,
                 name: fragment("?.nested.name", t.data),
                 arr: fragment("?.nested.arr", t.data)
               }
           ) == [
             %{mint: "123", name: "Test", arr: ["abc", "b=deb"]},
             %{mint: "325", name: "Test", arr: ["abc", "b=deb"]}
           ]
  end
end
