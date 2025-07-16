defmodule Ecto.Integration.JsonTest do
  use Ecto.Integration.Case
  import Ecto.Query, only: [from: 2]

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

    TestRepo.insert_all(TokenInfoSchema, [
      %{
        data: %{
          "authorities" => [
            %{"address" => "2wmVCSfPxGPjrnMMn7rchp4uaeoTqN39mXFC2zhPdri9", "scopes" => ["full"]}
          ],
          "burnt" => false,
          "compression" => %{
            "asset_hash" => "",
            "compressed" => false,
            "creator_hash" => "",
            "data_hash" => "",
            "eligible" => false,
            "leaf_id" => 0,
            "seq" => 0,
            "tree" => ""
          },
          "content" => %{
            "$schema" => "https://schema.metaplex.com/nft1.0.json",
            "files" => [
              %{
                "cdn_uri" =>
                  "https://cdn.helius-rpc.com/cdn-cgi/image//https://raw.githubusercontent.com/solana-labs/token-list/main/assets/mainnet/EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v/logo.png",
                "mime" => "image/png",
                "uri" =>
                  "https://raw.githubusercontent.com/solana-labs/token-list/main/assets/mainnet/EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v/logo.png"
              }
            ],
            "json_uri" => "",
            "links" => %{
              "image" =>
                "https://raw.githubusercontent.com/solana-labs/token-list/main/assets/mainnet/EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v/logo.png"
            },
            "metadata" => %{"name" => "USD Coin", "symbol" => "USDC"}
          },
          "creators" => [],
          "grouping" => [],
          "id" => "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
          "interface" => "FungibleToken",
          "mutable" => true,
          "ownership" => %{
            "delegate" => nil,
            "delegated" => false,
            "frozen" => false,
            "owner" => "",
            "ownership_model" => "token"
          },
          "royalty" => %{
            "basis_points" => 0,
            "locked" => false,
            "percent" => 0.0,
            "primary_sale_happened" => false,
            "royalty_model" => "creators",
            "target" => nil
          },
          "supply" => nil,
          "token_info" => %{
            "decimals" => 6,
            "freeze_authority" => "7dGbd2QZcCKcTndnHcTL8q7SMVXAkp688NTQYwrRCrar",
            "mint_authority" => "BJE5MMbqXjVwjAF7oxwPYXnTXDyspzZyt4vwenNw5ruG",
            "price_info" => %{"currency" => "USDC", "price_per_token" => 0.999867},
            "supply" => 8_276_375_974_708_499,
            "symbol" => "USDC",
            "token_program" => "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
          }
        },
        mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        created_at: ~N[2025-07-16 16:04:47]
      },
      %{
        data: %{
          "authorities" => [
            %{"address" => "2RtGg6fsFiiF1EQzHqbd66AhW7R5bWeQGpTbv2UMkCdW", "scopes" => ["full"]}
          ],
          "burnt" => false,
          "compression" => %{
            "asset_hash" => "",
            "compressed" => false,
            "creator_hash" => "",
            "data_hash" => "",
            "eligible" => false,
            "leaf_id" => 0,
            "seq" => 0,
            "tree" => ""
          },
          "content" => %{
            "$schema" => "https://schema.metaplex.com/nft1.0.json",
            "files" => [
              %{
                "cdn_uri" =>
                  "https://cdn.helius-rpc.com/cdn-cgi/image//https://madlads.s3.us-west-2.amazonaws.com/images/8420.png",
                "mime" => "image/png",
                "uri" => "https://madlads.s3.us-west-2.amazonaws.com/images/8420.png"
              },
              %{
                "cdn_uri" =>
                  "https://cdn.helius-rpc.com/cdn-cgi/image//https://arweave.net/qJ5B6fx5hEt4P7XbicbJQRyTcbyLaV-OQNA1KjzdqOQ/0.png",
                "mime" => "image/png",
                "uri" => "https://arweave.net/qJ5B6fx5hEt4P7XbicbJQRyTcbyLaV-OQNA1KjzdqOQ/0.png"
              }
            ],
            "json_uri" => "https://madlads.s3.us-west-2.amazonaws.com/json/8420.json",
            "links" => %{
              "external_url" => "https://madlads.com",
              "image" => "https://madlads.s3.us-west-2.amazonaws.com/images/8420.png"
            },
            "metadata" => %{
              "attributes" => [
                %{"trait_type" => "Gender", "value" => "Male"},
                %{"trait_type" => "Type", "value" => "King"},
                %{"trait_type" => "Expression", "value" => "Royal"},
                %{"trait_type" => "Hat", "value" => "Mad Crown"},
                %{"trait_type" => "Eyes", "value" => "Madness"},
                %{"trait_type" => "Clothing", "value" => "Mad Armor"},
                %{"trait_type" => "Background", "value" => "Royal Rug"}
              ],
              "description" => "Fock it.",
              "name" => "Mad Lads #8420",
              "symbol" => "MAD",
              "token_standard" => "ProgrammableNonFungible"
            }
          },
          "creators" => [
            %{
              "address" => "5XvhfmRjwXkGp3jHGmaKpqeerNYjkuZZBYLVQYdeVcRv",
              "share" => 0,
              "verified" => true
            },
            %{
              "address" => "2RtGg6fsFiiF1EQzHqbd66AhW7R5bWeQGpTbv2UMkCdW",
              "share" => 100,
              "verified" => true
            }
          ],
          "grouping" => [
            %{
              "group_key" => "collection",
              "group_value" => "J1S9H3QjnRtBbbuD4HjPV6RpRhwuk4zKbxsnCHuTgh9w"
            }
          ],
          "id" => "F9Lw3ki3hJ7PF9HQXsBzoY8GyE6sPoEZZdXJBsTTD2rk",
          "interface" => "ProgrammableNFT",
          "mutable" => true,
          "ownership" => %{
            "delegate" => nil,
            "delegated" => false,
            "frozen" => true,
            "owner" => "D3ftM66SZMdbCHiV9wBAFxoqqA8ex76nJnmVLbGy6vwp",
            "ownership_model" => "single"
          },
          "royalty" => %{
            "basis_points" => 420,
            "locked" => false,
            "percent" => 0.042,
            "primary_sale_happened" => true,
            "royalty_model" => "creators",
            "target" => nil
          },
          "supply" => %{
            "edition_nonce" => 254,
            "print_current_supply" => 0,
            "print_max_supply" => 0
          },
          "token_info" => %{
            "decimals" => 0,
            "freeze_authority" => "TdMA45ZnakQCBt5XUvm7ib2htKuTWdcgGKu1eUGrDyJ",
            "mint_authority" => "TdMA45ZnakQCBt5XUvm7ib2htKuTWdcgGKu1eUGrDyJ",
            "supply" => 1,
            "token_program" => "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
          }
        },
        mint: "F9Lw3ki3hJ7PF9HQXsBzoY8GyE6sPoEZZdXJBsTTD2rk",
        created_at: ~N[2025-07-16 16:04:47]
      }
    ])

    assert TestRepo.all(
             from t in TokenInfoSchema,
               order_by: t.created_at,
               select: %{
                 mint: t.mint,
                 basis_points: fragment("?.royalty.basis_points::String", t.data)
               }
           ) == [
             %{mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", basis_points: "0"},
             %{mint: "F9Lw3ki3hJ7PF9HQXsBzoY8GyE6sPoEZZdXJBsTTD2rk", basis_points: "420"}
           ]
  end
end
