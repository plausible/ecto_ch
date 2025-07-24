defmodule Mix.Tasks.Ecto.Ch.SchemaTest do
  use ExUnit.Case
  import ExUnit.CaptureIO

  test "run/1 help" do
    help =
      capture_io(fn ->
        Mix.Tasks.Ecto.Ch.Schema.run([])
      end)

    assert help == """
           Shows an Ecto schema hint for a ClickHouse table.

           Examples:

               $ mix ecto.ch.schema
               $ mix ecto.ch.schema system.numbers
               $ mix ecto.ch.schema system.numbers --repo MyApp.Repo

           """
  end

  describe "run/1" do
    setup do
      put_env_reset(:ecto_ch, :ecto_repos, [Ecto.Integration.TestRepo])
    end

    test "system.numbers" do
      schema =
        capture_io(fn ->
          Mix.Tasks.Ecto.Ch.Schema.run(["system.numbers"])
        end)

      assert schema == """
             @primary_key false
             schema "numbers" do
               field :number, Ch, type: "UInt64"
             end
             """
    end

    test "products" do
      schema =
        capture_io(fn ->
          Mix.Tasks.Ecto.Ch.Schema.run(["products"])
        end)

      assert schema == """
             @primary_key false
             schema "products" do
               field :id, Ch, type: "UInt64"
               field :account_id, Ch, type: "UInt64"
               field :name, :string
               field :description, :string
               field :external_id, Ecto.UUID
               field :tags, {:array, :string}
               field :approved_at, Ch, type: "DateTime"
               field :price, Ch, type: "Decimal(18, 2)"
               field :inserted_at, Ch, type: "DateTime"
               field :updated_at, Ch, type: "DateTime"
             end
             """
    end

    @tag :time
    test "all types" do
      Ecto.Integration.TestRepo.query!(
        """
        CREATE TABLE all_types (
          uint8 UInt8,
          uint16 UInt16,
          uint32 UInt32,
          uint64 UInt64,
          uint128 UInt128,
          uint256 UInt256,
          int8 Int8,
          int16 Int16,
          int32 Int32,
          int64 Int64,
          int128 Int128,
          int256 Int256,
          float32 Float32,
          float64 Float64,
          decimal32 Decimal32(4),
          decimal64 Decimal64(8),
          decimal128 Decimal128(16),
          decimal256 Decimal256(32),
          bool Bool,
          string String,
          fixed_string FixedString(32),
          date Date,
          date32 Date32,
          time Time,
          time64 Time64(6),
          datetime DateTime,
          datetime_europe DateTime('Europe/Vienna'),
          datetime64 DateTime64(6, 'UTC'),
          uuid UUID,
          ipv4 IPv4,
          ipv6 IPv6,
          enum Enum8('hello' = 1, 'world' = 2),
          enum16 Enum16('hello' = 1, 'world' = 2),
          nullable_string Nullable(String),
          nullable_int64 Nullable(Int64),
          array_string Array(String),
          array_int Array(UInt64),
          array_array_string Array(Array(String)),
          tuple Tuple(String, UInt64),
          named_tuple Tuple(name String, value UInt64),
          map Map(String, UInt64),
          json JSON,
          dynamic Dynamic,
          dynamic_max_10 Dynamic(max_types = 10),
          variant Variant(String, UInt64, Array(String), Map(String, UInt64)),
          low_cardinality_string LowCardinality(String),
          array_tuple_dynamic Array(Tuple(a LowCardinality(String), b LowCardinality(String), c LowCardinality(String), d Dynamic))
        ) ENGINE = MergeTree ORDER BY tuple()
        """,
        _no_params = [],
        settings: [enable_time_time64_type: 1]
      )

      on_exit(fn -> Ecto.Integration.TestRepo.query!("DROP TABLE all_types") end)

      schema =
        capture_io(fn ->
          Mix.Tasks.Ecto.Ch.Schema.run(["all_types"])
        end)

      assert schema == """
             @primary_key false
             schema "all_types" do
               field :uint8, Ch, type: "UInt8"
               field :uint16, Ch, type: "UInt16"
               field :uint32, Ch, type: "UInt32"
               field :uint64, Ch, type: "UInt64"
               field :uint128, Ch, type: "UInt128"
               field :uint256, Ch, type: "UInt256"
               field :int8, Ch, type: "Int8"
               field :int16, Ch, type: "Int16"
               field :int32, Ch, type: "Int32"
               field :int64, Ch, type: "Int64"
               field :int128, Ch, type: "Int128"
               field :int256, Ch, type: "Int256"
               field :float32, Ch, type: "Float32"
               field :float64, Ch, type: "Float64"
               field :decimal32, Ch, type: "Decimal(9, 4)"
               field :decimal64, Ch, type: "Decimal(18, 8)"
               field :decimal128, Ch, type: "Decimal(38, 16)"
               field :decimal256, Ch, type: "Decimal(76, 32)"
               field :bool, :boolean
               field :string, :string
               field :fixed_string, Ch, type: "FixedString(32)"
               field :date, :date
               field :date32, Ch, type: "Date32"
               field :time, Ch, type: "Time"
               field :time64, Ch, type: "Time64(6)"
               field :datetime, Ch, type: "DateTime"
               field :datetime_europe, Ch, type: "DateTime('Europe/Vienna')"
               field :datetime64, Ch, type: "DateTime64(6, 'UTC')"
               field :uuid, Ecto.UUID
               field :ipv4, Ch, type: "IPv4"
               field :ipv6, Ch, type: "IPv6"
               field :enum, Ch, type: "Enum8('hello' = 1, 'world' = 2)"
               field :enum16, Ch, type: "Enum16('hello' = 1, 'world' = 2)"
               field :nullable_string, Ch, type: "Nullable(String)"
               field :nullable_int64, Ch, type: "Nullable(Int64)"
               field :array_string, {:array, :string}
               field :array_int, {:array, Ch}, type: "UInt64"
               field :array_array_string, {:array, {:array, :string}}
               field :tuple, Ch, type: "Tuple(String, UInt64)"
               field :named_tuple, Ch, type: "Tuple(String, UInt64)"
               field :map, Ch, type: "Map(String, UInt64)"
               field :json, Ch, type: "JSON"
               field :dynamic, Ch, type: "Dynamic"
               field :dynamic_max_10, Ch, type: "Dynamic"
               field :variant, Ch, type: "Variant(Array(String), Map(String, UInt64), String, UInt64)"
               field :low_cardinality_string, Ch, type: "LowCardinality(String)"
               field :array_tuple_dynamic, {:array, Ch}, type: "Tuple(LowCardinality(String), LowCardinality(String), LowCardinality(String), Dynamic)"
             end
             """
    end
  end

  describe "run/1 custom repo flags" do
    test "-r" do
      schema =
        capture_io(fn ->
          Mix.Tasks.Ecto.Ch.Schema.run(["events", "-r", "Ecto.Integration.TestRepo"])
        end)

      assert schema == """
             @primary_key false
             schema "events" do
               field :id, Ch, type: "UInt64"
               field :domain, :string
               field :type, :string
               field :tags, {:array, :string}
               field :session_id, Ch, type: "UInt64"
               field :inserted_at, Ch, type: "DateTime"
             end
             """
    end

    test "--repo" do
      schema =
        capture_io(fn ->
          Mix.Tasks.Ecto.Ch.Schema.run(["accounts", "--repo", "Ecto.Integration.TestRepo"])
        end)

      assert schema == """
             @primary_key false
             schema "accounts" do
               field :id, Ch, type: "UInt64"
               field :name, :string
               field :email, :string
               field :inserted_at, Ch, type: "DateTime"
               field :updated_at, Ch, type: "DateTime"
             end
             """
    end
  end

  test "build_type/1" do
    import Mix.Tasks.Ecto.Ch.Schema, only: [build_field: 2]

    assert build_field("metric", "String") ==
             ~s[field :metric, :string]

    assert build_field("metric", "Array(String)") ==
             ~s[field :metric, {:array, :string}]

    assert build_field("metric", "Array(UInt64)") ==
             ~s[field :metric, {:array, Ch}, type: "UInt64"]

    assert build_field("metric", "Array(Array(UInt64))") ==
             ~s[field :metric, {:array, {:array, Ch}}, type: "UInt64"]

    assert build_field("metric", "Array(Array(String))") ==
             ~s[field :metric, {:array, {:array, :string}}]

    assert build_field("metric", "Array(Tuple(String, UInt64))") ==
             ~s[field :metric, {:array, Ch}, type: "Tuple(String, UInt64)"]
  end

  defp put_env_reset(app, key, value) do
    prev = Application.get_env(app, key)
    :ok = Application.put_env(app, key, value)

    on_exit(fn ->
      if prev do
        Application.put_env(app, key, prev)
      else
        Application.delete_env(app, key)
      end
    end)
  end
end
