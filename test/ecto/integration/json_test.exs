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
          "edge cases" => %{
            "space key" => "space",
            "dash-key" => "dash",
            "1number" => "number",
            "has.dot" => "dot",
            "'single" => "single",
            "\"double" => "double",
            "back\\slash" => "backslash",
            "a`b" => "backtick",
            "backspace\bkey" => "backspace",
            "form\fkey" => "form",
            "tab\tkey" => "tab",
            "line\nbreak" => "newline",
            "carriage\rreturn" => "carriage",
            "comma,key" => "comma",
            "colon:key" => "colon",
            "slash/key" => "slash",
            "bracket[key]" => "bracket"
          }
        },
        time: ~N[2023-10-01 13:00:00]
      }
    ])

    assert TestRepo.all(
             from s in SemiStructured,
               select: %{
                 from: json_extract_path(s.json, ["from"]),
                 name: json_extract_path(s.json, ["nested", "name"]),
                 edge_space: json_extract_path(s.json, ["edge cases", "space key"]),
                 edge_dash: json_extract_path(s.json, ["edge cases", "dash-key"]),
                 edge_number: json_extract_path(s.json, ["edge cases", "1number"]),
                 edge_dot: json_extract_path(s.json, ["edge cases", "has.dot"]),
                 edge_single_quote: json_extract_path(s.json, ["edge cases", "'single"]),
                 edge_double_quote: json_extract_path(s.json, ["edge cases", "\"double"]),
                 edge_backslash: json_extract_path(s.json, ["edge cases", "back\\slash"]),
                 edge_backtick: json_extract_path(s.json, ["edge cases", "a`b"]),
                 edge_backspace: json_extract_path(s.json, ["edge cases", "backspace\bkey"]),
                 edge_form: json_extract_path(s.json, ["edge cases", "form\fkey"]),
                 edge_tab: json_extract_path(s.json, ["edge cases", "tab\tkey"]),
                 edge_newline: json_extract_path(s.json, ["edge cases", "line\nbreak"]),
                 edge_carriage: json_extract_path(s.json, ["edge cases", "carriage\rreturn"]),
                 edge_comma: json_extract_path(s.json, ["edge cases", "comma,key"]),
                 edge_colon: json_extract_path(s.json, ["edge cases", "colon:key"]),
                 edge_slash: json_extract_path(s.json, ["edge cases", "slash/key"]),
                 edge_bracket: json_extract_path(s.json, ["edge cases", "bracket[key]"]),
                 bracket_name: s.json["nested"]["name"]
               }
           ) == [
             %{
               from: "insert_all",
               name: "Test",
               edge_space: "space",
               edge_dash: "dash",
               edge_number: "number",
               edge_dot: "dot",
               edge_single_quote: "single",
               edge_double_quote: "double",
               edge_backslash: "backslash",
               edge_backtick: "backtick",
               edge_backspace: "backspace",
               edge_form: "form",
               edge_tab: "tab",
               edge_newline: "newline",
               edge_carriage: "carriage",
               edge_comma: "comma",
               edge_colon: "colon",
               edge_slash: "slash",
               edge_bracket: "bracket",
               bracket_name: "Test"
             }
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
