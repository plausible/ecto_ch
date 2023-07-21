defmodule Ch.TypeTest do
  use Ecto.Integration.Case

  import Ecto.Query
  import Bitwise

  import Ecto.Integration.TestRepo, only: [query!: 1, query!: 3, insert_all: 2, all: 1]

  describe "field options" do
    defmodule Defaults do
      use Ecto.Schema

      @primary_key false
      schema "ch_defaults" do
        field(:i8, Ch, type: "Int8", default: 42)
        field(:string, Ch, type: "String", default: "42")
      end
    end

    # TODO
    @tag skip: true
    test ":default" do
      query!("create table ch_defaults (`i8` Int8, `string` String) engine Memory")

      assert {1, _} = insert_all(Defaults, _rows = [[i8: nil, string: nil]])
      assert Defaults |> all() |> unstruct() == [%{i8: 42, string: "42"}]
    end
  end

  # TODO what if overlow
  describe "Int / UInt" do
    setup do
      query!("""
      create table ch_ints(
        `i8`   Int8,
        `i16`  Int16,
        `i32`  Int32,
        `i64`  Int64,
        `i128` Int128,
        `i256` Int256,
        `u8`   UInt8,
        `u16`  UInt16,
        `u32`  UInt32,
        `u64`  UInt64,
        `u128` UInt128,
        `u256` UInt256
      ) engine Memory
      """)

      on_exit(fn -> query!("truncate ch_ints") end)
    end

    defmodule Ints do
      use Ecto.Schema

      @primary_key false
      schema "ch_ints" do
        field(:i8, Ch, type: "Int8")
        field(:i16, Ch, type: "Int16")
        field(:i32, Ch, type: "Int32")
        field(:i64, Ch, type: "Int64")
        field(:i128, Ch, type: "Int128")
        field(:i256, Ch, type: "Int256")
        field(:u8, Ch, type: "UInt8")
        field(:u16, Ch, type: "UInt16")
        field(:u32, Ch, type: "UInt32")
        field(:u64, Ch, type: "UInt64")
        field(:u128, Ch, type: "UInt128")
        field(:u256, Ch, type: "UInt256")
      end
    end

    test "insert_all" do
      min = [
        i8: -(1 <<< 7),
        i16: -(1 <<< 15),
        i32: -(1 <<< 31),
        i64: -(1 <<< 63),
        i128: -(1 <<< 127),
        i256: -(1 <<< 255),
        u8: 0,
        u16: 0,
        u32: 0,
        u64: 0,
        u128: 0,
        u256: 0
      ]

      zeros = Enum.map(Ints.__schema__(:fields), fn field -> {field, 0} end)

      max = [
        i8: (1 <<< 7) - 1,
        i16: (1 <<< 15) - 1,
        i32: (1 <<< 31) - 1,
        i64: (1 <<< 63) - 1,
        i128: (1 <<< 127) - 1,
        i256: (1 <<< 255) - 1,
        u8: (1 <<< 8) - 1,
        u16: (1 <<< 16) - 1,
        u32: (1 <<< 32) - 1,
        u64: (1 <<< 64) - 1,
        u128: (1 <<< 128) - 1,
        u256: (1 <<< 256) - 1
      ]

      nulls = Enum.map(Ints.__schema__(:fields), fn field -> {field, nil} end)

      assert {4, _} = insert_all(Ints, _rows = [min, zeros, max, nulls])

      assert Ints
             |> order_by([i], i.i8)
             |> all()
             |> unstruct() == [
               %{
                 i128: -170_141_183_460_469_231_731_687_303_715_884_105_728,
                 i16: -32768,
                 i256:
                   -57_896_044_618_658_097_711_785_492_504_343_953_926_634_992_332_820_282_019_728_792_003_956_564_819_968,
                 i32: -2_147_483_648,
                 i64: -9_223_372_036_854_775_808,
                 i8: -128,
                 u128: 0,
                 u16: 0,
                 u256: 0,
                 u32: 0,
                 u64: 0,
                 u8: 0
               },
               %{
                 i128: 0,
                 i16: 0,
                 i256: 0,
                 i32: 0,
                 i64: 0,
                 i8: 0,
                 u128: 0,
                 u16: 0,
                 u256: 0,
                 u32: 0,
                 u64: 0,
                 u8: 0
               },
               %{
                 i128: 0,
                 i16: 0,
                 i256: 0,
                 i32: 0,
                 i64: 0,
                 i8: 0,
                 u128: 0,
                 u16: 0,
                 u256: 0,
                 u32: 0,
                 u64: 0,
                 u8: 0
               },
               %{
                 i128: 170_141_183_460_469_231_731_687_303_715_884_105_727,
                 i16: 32767,
                 i256:
                   57_896_044_618_658_097_711_785_492_504_343_953_926_634_992_332_820_282_019_728_792_003_956_564_819_967,
                 i32: 2_147_483_647,
                 i64: 9_223_372_036_854_775_807,
                 i8: 127,
                 u128: 340_282_366_920_938_463_463_374_607_431_768_211_455,
                 u16: 65535,
                 u256:
                   115_792_089_237_316_195_423_570_985_008_687_907_853_269_984_665_640_564_039_457_584_007_913_129_639_935,
                 u32: 4_294_967_295,
                 u64: 18_446_744_073_709_551_615,
                 u8: 255
               }
             ]
    end
  end

  # TODO what if overlow
  describe "Float" do
    setup do
      query!("create table ch_floats(f32 Float32, f64 Float64) engine Memory")
      on_exit(fn -> query!("truncate ch_floats") end)
    end

    defmodule Floats do
      use Ecto.Schema

      @primary_key false
      schema "ch_floats" do
        field(:f32, Ch, type: "Float32")
        field(:f64, Ch, type: "Float64")
      end
    end

    test "insert_all" do
      assert {4, _} =
               insert_all(
                 Floats,
                 _rows = [
                   [f32: nil, f64: nil],
                   # TODO
                   # [f32: 0, f64: 0],
                   [f32: 0.0, f64: 0.0],
                   [f32: -42.0, f64: -42.42],
                   [f32: 42.0, f64: 42.42]
                 ]
               )

      assert Floats |> order_by([f], f.f32) |> all() |> unstruct() == [
               %{f32: -42.0, f64: -42.42},
               %{f32: 0.0, f64: 0.0},
               %{f32: 0.0, f64: 0.0},
               %{f32: 42.0, f64: 42.42}
             ]
    end
  end

  # TODO what if overlow
  describe "Decimal" do
    setup do
      query!("""
      create table ch_decimals (
        d_15_10 Decimal(15, 10),
        d32_2 Decimal32(2),
        d64_4 Decimal64(4),
        d128_6 Decimal128(6),
        d256_8 Decimal256(8)
      ) engine Memory
      """)

      on_exit(fn -> query!("truncate ch_decimals") end)
    end

    defmodule Decimals do
      use Ecto.Schema

      @primary_key false
      schema "ch_decimals" do
        field(:d_15_10, Ch, type: "Decimal(15, 10)")
        field(:d32_2, Ch, type: "Decimal32(2)")
        field(:d64_4, Ch, type: "Decimal64(4)")
        field(:d128_6, Ch, type: "Decimal128(6)")
        field(:d256_8, Ch, type: "Decimal256(8)")
      end
    end

    test "insert_all" do
      assert {4, _} =
               insert_all(
                 Decimals,
                 _rows = [
                   Enum.map(Decimals.__schema__(:fields), fn field -> {field, nil} end),
                   Enum.map(Decimals.__schema__(:fields), fn field -> {field, 0} end),
                   [
                     d_15_10: Decimal.new("1.123456789"),
                     d32_2: Decimal.new("231.23"),
                     d64_4: Decimal.new("54321.2345"),
                     d128_6: Decimal.new("7654321.234567"),
                     d256_8: Decimal.new("1.1")
                   ],
                   [
                     d_15_10: Decimal.new("-1.123456789"),
                     d32_2: Decimal.new("-231.23"),
                     d64_4: Decimal.new("-54321.2345"),
                     d128_6: Decimal.new("-7654321.234567"),
                     d256_8: Decimal.new("-1.1")
                   ]
                 ]
               )

      assert Decimals |> order_by([d], d.d_15_10) |> all() |> unstruct() == [
               %{
                 d128_6: Decimal.new("-7654321.234567"),
                 d256_8: Decimal.new("-1.10000000"),
                 d32_2: Decimal.new("-231.23"),
                 d64_4: Decimal.new("-54321.2345"),
                 d_15_10: Decimal.new("-1.1234567890")
               },
               %{
                 d128_6: Decimal.new("0.000000"),
                 d256_8: Decimal.new("0E-8"),
                 d32_2: Decimal.new("0.00"),
                 d64_4: Decimal.new("0.0000"),
                 d_15_10: Decimal.new("0E-10")
               },
               %{
                 d128_6: Decimal.new("0.000000"),
                 d256_8: Decimal.new("0E-8"),
                 d32_2: Decimal.new("0.00"),
                 d64_4: Decimal.new("0.0000"),
                 d_15_10: Decimal.new("0E-10")
               },
               %{
                 d128_6: Decimal.new("7654321.234567"),
                 d256_8: Decimal.new("1.10000000"),
                 d32_2: Decimal.new("231.23"),
                 d64_4: Decimal.new("54321.2345"),
                 d_15_10: Decimal.new("1.1234567890")
               }
             ]
    end
  end

  describe "Bool" do
    setup do
      query!("create table ch_bools (ch_bool Bool, bool Bool) engine Memory")
      on_exit(fn -> query!("truncate ch_bools") end)
    end

    defmodule Bools do
      use Ecto.Schema

      @primary_key false
      schema "ch_bools" do
        field(:ch_bool, Ch, type: "Bool")
        field(:bool, :boolean)
      end
    end

    test "insert_all" do
      assert {3, _} =
               insert_all(
                 Bools,
                 _rows = [
                   [ch_bool: nil, bool: nil],
                   [ch_bool: false, bool: false],
                   [ch_bool: true, bool: true]
                 ]
               )

      assert Bools |> order_by([b], b.ch_bool) |> all() |> unstruct() == [
               %{bool: false, ch_bool: false},
               %{bool: false, ch_bool: false},
               %{bool: true, ch_bool: true}
             ]
    end
  end

  describe "String" do
    setup do
      query!("create table ch_strings (ch_string String, string String) engine Memory")
      on_exit(fn -> query!("truncate ch_strings") end)
    end

    defmodule Strings do
      use Ecto.Schema

      @primary_key false
      schema "ch_strings" do
        field(:ch_string, Ch, type: "String")
        field(:string, :string)
      end
    end

    test "insert_all" do
      assert {4, _} =
               insert_all(
                 Strings,
                 _rows = [
                   [ch_string: nil, string: nil],
                   [ch_string: "", string: ""],
                   [ch_string: "hello", string: "world"],
                   [ch_string: "\x61\xF0\x80\x80\x80b", string: "\x61\xF0\x80\x80\x80b"]
                 ]
               )

      assert Strings |> order_by([s], s.ch_string) |> all() |> unstruct() == [
               %{ch_string: "", string: ""},
               %{ch_string: "", string: ""},
               %{ch_string: "a�b", string: "a�b"},
               %{ch_string: "hello", string: "world"}
             ]
    end
  end

  describe "FixedString(n)" do
    setup do
      query!("create table ch_fixed_strings (f2 FixedString(2), f3 FixedString(3)) engine Memory")
      on_exit(fn -> query!("truncate ch_fixed_strings") end)
    end

    defmodule FixedStrings do
      use Ecto.Schema

      @primary_key false
      schema "ch_fixed_strings" do
        field(:f2, Ch, type: "FixedString(2)")
        field(:f3, Ch, type: "FixedString(3)")
      end
    end

    test "insert_all" do
      assert {4, _} =
               insert_all(FixedStrings, [
                 [f2: nil, f3: nil],
                 [f2: "", f3: ""],
                 [f2: "a", f3: "a"],
                 [f2: "ab", f3: "abc"]
               ])

      assert FixedStrings |> order_by([f], f.f2) |> all() |> unstruct() == [
               %{f2: <<0, 0>>, f3: <<0, 0, 0>>},
               %{f2: <<0, 0>>, f3: <<0, 0, 0>>},
               %{f2: <<?a, 0>>, f3: <<?a, 0, 0>>},
               %{f2: "ab", f3: "abc"}
             ]
    end
  end

  describe "UUID" do
    setup do
      query!("create table ch_uuids (`ch_uuid` UUID, `uuid` UUID) engine Memory")
      on_exit(fn -> query!("truncate ch_uuids") end)
    end

    defmodule UUIDs do
      use Ecto.Schema

      @primary_key false
      schema "ch_uuids" do
        field(:ch_uuid, Ch, type: "UUID")
        field(:uuid, Ecto.UUID)
      end
    end

    test "insert_all" do
      uuid = Ecto.UUID.bingenerate()
      uuid_hex = Ecto.UUID.cast!(uuid)

      assert {2, nil} =
               insert_all(
                 UUIDs,
                 [
                   [ch_uuid: nil, uuid: nil],
                   [ch_uuid: uuid_hex, uuid: uuid_hex]
                   # TODO
                   #  [ch_uuid: uuid, uuid: uuid]
                 ]
               )

      assert UUIDs |> order_by([u], u.ch_uuid) |> all() |> unstruct() == [
               %{
                 ch_uuid: "00000000-0000-0000-0000-000000000000",
                 uuid: "00000000-0000-0000-0000-000000000000"
               },
               %{ch_uuid: uuid_hex, uuid: uuid_hex}
             ]
    end
  end

  describe "Date" do
    setup do
      query!("create table ch_dates (ch_date Date, date Date) engine Memory")
      on_exit(fn -> query!("truncate ch_dates") end)
    end

    defmodule Dates do
      use Ecto.Schema

      @primary_key false
      schema "ch_dates" do
        field(:ch_date, Ch, type: "Date")
        field(:date, :date)
      end
    end

    test "insert_all" do
      today = Date.utc_today()

      assert {2, _} =
               insert_all(Dates, [
                 [ch_date: nil, date: nil],
                 # [ch_date: "2000-01-01", date: "2000-01-01"],
                 [ch_date: today, date: today]
               ])

      assert Dates |> order_by([d], d.ch_date) |> all() |> unstruct() == [
               %{ch_date: ~D[1970-01-01], date: ~D[1970-01-01]},
               %{ch_date: today, date: today}
             ]
    end
  end

  describe "Date32" do
    setup do
      query!("create table ch_dates32 (date Date32) engine Memory")
      on_exit(fn -> query!("truncate ch_dates32") end)
    end

    defmodule Dates32 do
      use Ecto.Schema

      @primary_key false
      schema "ch_dates32" do
        field(:date, Ch, type: "Date32")
      end
    end

    test "insert_all" do
      today = Date.utc_today()

      assert {2, _} =
               insert_all(Dates32, [
                 [date: nil],
                 [date: today]
               ])

      assert Dates32 |> order_by([d], d.date) |> all() |> unstruct() == [
               %{date: ~D[1970-01-01]},
               %{date: today}
             ]
    end
  end

  describe "DateTime" do
    setup do
      query!("""
      create table ch_datetimes (
        ch_datetime DateTime,
        ch_datetime_utc DateTime('UTC'),
        naive_datetime DateTime,
        utc_datetime DateTime
      ) engine Memory
      """)

      on_exit(fn -> query!("truncate ch_datetimes") end)
    end

    defmodule Datetimes do
      use Ecto.Schema

      @primary_key false
      schema "ch_datetimes" do
        field(:ch_datetime, Ch, type: "DateTime")
        field(:ch_datetime_utc, Ch, type: "DateTime('UTC')")
        field(:naive_datetime, :naive_datetime)
        field(:utc_datetime, :utc_datetime)
      end
    end

    test "insert_all" do
      utc_now = DateTime.utc_now() |> DateTime.truncate(:second)
      naive_now = NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)

      assert {2, _} =
               insert_all(Datetimes, [
                 Enum.map(Datetimes.__schema__(:fields), fn field -> {field, nil} end),
                 [
                   ch_datetime: naive_now,
                   ch_datetime_utc: utc_now,
                   naive_datetime: naive_now,
                   utc_datetime: utc_now
                 ]
               ])

      assert Datetimes |> order_by([d], d.ch_datetime) |> all() |> unstruct() == [
               %{
                 ch_datetime: ~N[1970-01-01 00:00:00],
                 ch_datetime_utc: ~U[1970-01-01 00:00:00Z],
                 naive_datetime: ~N[1970-01-01 00:00:00],
                 utc_datetime: ~U[1970-01-01 00:00:00Z]
               },
               %{
                 ch_datetime: naive_now,
                 ch_datetime_utc: utc_now,
                 naive_datetime: naive_now,
                 utc_datetime: utc_now
               }
             ]
    end
  end

  describe "DateTime64" do
    setup do
      query!("""
      create table ch_datetimes64 (
        ch_3 DateTime64(3),
        ch_6 DateTime64(6),
        ch_5_utc DateTime(5, 'UTC'),
        naive_datetime_usec DateTime64(6),
        utc_datetime_usec DateTime64(6)
      ) engine Memory
      """)

      on_exit(fn -> query!("truncate ch_datetimes64") end)
    end

    defmodule Datetimes64 do
      use Ecto.Schema

      @primary_key false
      schema "ch_datetimes64" do
        field(:ch_3, Ch, type: "DateTime64(3)")
        field(:ch_6, Ch, type: "DateTime64(6)")
        field(:ch_5_utc, Ch, type: "DateTime64(5, 'UTC')")
        field(:naive_datetime_usec, :naive_datetime_usec)
        field(:utc_datetime_usec, :utc_datetime_usec)
      end
    end

    test "insert_all" do
      utc_now = ~U[2023-04-24 09:02:00.975319Z]
      naive_now = ~N[2023-04-24 09:02:00.975319]

      assert {2, _} =
               insert_all(Datetimes64, [
                 _nils = Enum.map(Datetimes64.__schema__(:fields), fn field -> {field, nil} end),
                 _now = [
                   ch_3: naive_now,
                   ch_6: naive_now,
                   ch_5_utc: utc_now,
                   naive_datetime_usec: naive_now,
                   utc_datetime_usec: utc_now
                 ]
               ])

      assert Datetimes64 |> order_by([d], d.ch_3) |> all() |> unstruct() == [
               %{
                 ch_3: ~N[1970-01-01 00:00:00.000000],
                 ch_6: ~N[1970-01-01 00:00:00.000000],
                 ch_5_utc: ~U[1970-01-01 00:00:00.00000Z],
                 naive_datetime_usec: ~N[1970-01-01 00:00:00.000000],
                 utc_datetime_usec: ~U[1970-01-01 00:00:00.000000Z]
               },
               %{
                 ch_3: ~N[2023-04-24 09:02:00.975000],
                 ch_6: naive_now,
                 ch_5_utc: ~U[2023-04-24 09:02:00.97531Z],
                 naive_datetime_usec: naive_now,
                 utc_datetime_usec: utc_now
               }
             ]
    end
  end

  describe "Enum" do
    setup do
      query!("""
      create table ch_enums (
        e8 Enum8('hello' = 1, 'world' = 2),
        e16 Enum16('hello' = -100, 'world' = 1000)
      ) engine Memory
      """)

      on_exit(fn -> query!("truncate ch_enums") end)
    end

    defmodule Enums do
      use Ecto.Schema

      @primary_key false
      schema "ch_enums" do
        field(:e8, Ch, type: "Enum8('hello' = 1, 'world' = 2)")
        field(:e16, Ch, type: "Enum16('hello' = -100, 'world' = 1000)")
      end
    end

    test "insert_all" do
      assert {2, _} =
               insert_all(Enums, [
                 _hello = [e8: "hello", e16: "hello"],
                 _world = [e8: "world", e16: "world"]
               ])

      assert Enums |> order_by([e], e.e8) |> all() |> unstruct() == [
               %{e8: "hello", e16: "hello"},
               %{e8: "world", e16: "world"}
             ]
    end
  end

  describe "LowCardinality" do
    setup do
      query!(
        """
        create table ch_low_cardinalities (
          string LowCardinality(String),
          fixed LowCardinality(FixedString(16)),
          date LowCardinality(Date),
          datetime LowCardinality(DateTime),
          int16 LowCardinality(Int16)
        ) engine Memory
        """,
        _params = [],
        settings: [allow_suspicious_low_cardinality_types: 1]
      )

      on_exit(fn -> query!("truncate ch_low_cardinalities") end)
    end

    defmodule LowCardinalities do
      use Ecto.Schema

      @primary_key false
      schema "ch_low_cardinalities" do
        field(:string, Ch, type: "LowCardinality(String)")
        field(:fixed, Ch, type: "LowCardinality(FixedString(16))")
        field(:date, Ch, type: "LowCardinality(Date)")
        field(:datetime, Ch, type: "LowCardinality(DateTime)")
        field(:int16, Ch, type: "LowCardinality(Int16)")
      end
    end

    test "insert_all" do
      today = Date.utc_today()
      now = NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)

      assert {2, _} =
               insert_all(LowCardinalities, [
                 _nils =
                   Enum.map(LowCardinalities.__schema__(:fields), fn field -> {field, nil} end),
                 [
                   string: "ABC",
                   fixed: String.duplicate("ABCD", 4),
                   date: today,
                   datetime: now,
                   int16: 123
                 ]
               ])

      assert LowCardinalities |> order_by([l], l.string) |> all() |> unstruct() == [
               %{
                 date: ~D[1970-01-01],
                 datetime: ~N[1970-01-01 00:00:00],
                 fixed: <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>,
                 int16: 0,
                 string: ""
               },
               %{
                 date: today,
                 datetime: now,
                 fixed: "ABCDABCDABCDABCD",
                 int16: 123,
                 string: "ABC"
               }
             ]
    end
  end

  describe "Array" do
    setup do
      query!("""
      create table if not exists ch_arrays (
        strings Array(String),
        maybe_strings Array(Nullable(String)),
        ints Array(Int32),
        uuids Array(UUID),
        arrays_of_strings Array(Array(String))
      ) engine Memory
      """)

      on_exit(fn -> query!("truncate ch_arrays") end)
    end

    defmodule Arrays do
      use Ecto.Schema

      @primary_key false
      schema "ch_arrays" do
        field(:strings, Ch, type: "Array(String)")
        field(:maybe_strings, Ch, type: "Array(Nullable(String))")
        field(:ints, Ch, type: "Array(Int32)")
        field(:uuids, Ch, type: "Array(UUID)")
        field(:arrays_of_strings, Ch, type: "Array(Array(String))")
      end
    end

    test "insert_all (ch)" do
      uuid = Ecto.UUID.generate()

      assert {4, _} =
               insert_all(Arrays, [
                 _nils = Enum.map(Arrays.__schema__(:fields), fn field -> {field, nil} end),
                 _empty = Enum.map(Arrays.__schema__(:fields), fn field -> {field, []} end),
                 _with_nil = Enum.map(Arrays.__schema__(:fields), fn field -> {field, [nil]} end),
                 [
                   strings: ["hello"],
                   maybe_strings: [nil, "hello"],
                   ints: [-42, 42],
                   uuids: [uuid],
                   arrays_of_strings: [nil, [], ["hello", nil]]
                 ]
               ])

      assert Arrays |> order_by([a], a.strings) |> all() |> unstruct() == [
               %{arrays_of_strings: [], ints: [], maybe_strings: [], strings: [], uuids: []},
               %{arrays_of_strings: [], ints: [], maybe_strings: [], strings: [], uuids: []},
               %{
                 arrays_of_strings: [[]],
                 ints: [0],
                 maybe_strings: [nil],
                 strings: [""],
                 uuids: ["00000000-0000-0000-0000-000000000000"]
               },
               %{
                 arrays_of_strings: [[], [], ["hello", ""]],
                 ints: [-42, 42],
                 maybe_strings: [nil, "hello"],
                 strings: ["hello"],
                 uuids: [uuid]
               }
             ]
    end

    defmodule EctoArrays do
      use Ecto.Schema

      @primary_key false
      schema "ch_arrays" do
        field(:strings, {:array, :string})
        field(:uuids, {:array, Ecto.UUID})
        field(:arrays_of_strings, {:array, {:array, :string}})
      end
    end

    test "insert_all ({:array, ecto_type})" do
      uuid = Ecto.UUID.generate()

      assert {4, _} =
               insert_all(EctoArrays, [
                 _nils = Enum.map(EctoArrays.__schema__(:fields), fn field -> {field, nil} end),
                 _empty = Enum.map(EctoArrays.__schema__(:fields), fn field -> {field, []} end),
                 _with_nil =
                   Enum.map(EctoArrays.__schema__(:fields), fn field -> {field, [nil]} end),
                 [
                   strings: ["hello"],
                   uuids: [uuid],
                   arrays_of_strings: [nil, [], ["hello", nil]]
                 ]
               ])

      assert EctoArrays |> order_by([a], a.strings) |> all() |> unstruct() == [
               %{arrays_of_strings: [], strings: [], uuids: []},
               %{arrays_of_strings: [], strings: [], uuids: []},
               %{
                 arrays_of_strings: [[]],
                 strings: [""],
                 uuids: ["00000000-0000-0000-0000-000000000000"]
               },
               %{arrays_of_strings: [[], [], ["hello", ""]], strings: ["hello"], uuids: [uuid]}
             ]
    end
  end

  describe "Tuple" do
    setup do
      query!("""
      create table ch_tuples (
        t1 Tuple(String),
        t2 Tuple(String, Int32),
        t3 Tuple(String, Array(String), Int32)
      ) engine Memory
      """)

      on_exit(fn -> query!("truncate ch_tuples") end)
    end

    defmodule Tuples do
      use Ecto.Schema

      @primary_key false
      schema "ch_tuples" do
        field(:t1, Ch, type: "Tuple(String)")
        field(:t2, Ch, type: "Tuple(String, Int32)")
        field(:t3, Ch, type: "Tuple(String, Array(String), Int32)")
      end
    end

    test "insert_all" do
      assert {2, _} =
               insert_all(Tuples, [
                 [t1: nil, t2: nil, t3: nil],
                 [t1: {"hello"}, t2: {"hello", 42}, t3: {"hello", ["world"], 42}]
               ])

      assert Tuples |> order_by([t], t.t1) |> all() |> unstruct() == [
               %{t1: {""}, t2: {"", 0}, t3: {"", [], 0}},
               %{t1: {"hello"}, t2: {"hello", 42}, t3: {"hello", ["world"], 42}}
             ]

      # TODO
      # assert Tuples |> where([t], "hell" in t.t1) |> all() |> unstruct() == []
      # assert Tuples |> where([t], ^"hell" in t.t1) |> all() |> unstruct() == []
      # assert Tuples |> where([t], 42 in t.t2) |> all() |> unstruct() == []
      # assert Tuples |> where([t], ^42 in t.t2) |> all() |> unstruct() == []
      # assert Tuples |> where([t], ["world"] in t.t3) |> all() |> unstruct() == []
      # assert Tuples |> where([t], ^["world"] in t.t3) |> all() |> unstruct() == []
    end
  end

  describe "Map" do
    setup do
      query!("""
      create table ch_maps (
        string_int Map(String, Int64),
        date_string Map(Date, String),
        int_strings Map(UInt64, Array(String))
      ) engine Memory
      """)

      on_exit(fn -> query!("truncate ch_maps") end)
    end

    defmodule Maps do
      use Ecto.Schema

      @primary_key false
      schema "ch_maps" do
        field(:string_int, Ch, type: "Map(String, Int64)")
        field(:date_string, Ch, type: "Map(Date, String)")
        field(:int_strings, Ch, type: "Map(UInt64, Array(String))")
      end
    end

    test "insert_all" do
      today = Date.utc_today()

      assert {2, _} =
               insert_all(Maps, [
                 [string_int: nil, date_string: nil, int_strings: nil],
                 [
                   string_int: %{"abc" => 123, "xyz" => -321},
                   date_string: %{today => "good day"},
                   int_strings: %{42 => ["a", "b", "c"]}
                 ]
               ])

      assert Maps |> order_by([m], m.string_int) |> all() |> unstruct() == [
               %{date_string: %{}, int_strings: %{}, string_int: %{}},
               %{
                 date_string: %{today => "good day"},
                 int_strings: %{42 => ["a", "b", "c"]},
                 string_int: %{"abc" => 123, "xyz" => -321}
               }
             ]
    end
  end

  describe "Nullable" do
    setup do
      query!("""
      create table ch_nullables (
        string Nullable(String),
        fixed_string Nullable(FixedString(2)),
        float Nullable(Float32),
        uuid Nullable(UUID),
        int Nullable(Int32),
        date Nullable(Date),
        date32 Nullable(Date32),
        datetime Nullable(DateTime),
        datetime_utc Nullable(DateTime('UTC')),
        datetime64 Nullable(DateTime64(6)),
        datetime64_utc Nullable(DateTime64(6, 'UTC')),
        enum8 Nullable(Enum8('hello' = 1, 'world' = 2)),
        enum16 Nullable(Enum16('hello' = 1, 'world' = 2)),
        decimal Nullable(Decimal64(2))
      ) engine Memory
      """)

      on_exit(fn -> query!("truncate ch_nullables") end)
    end

    defmodule Nullables do
      use Ecto.Schema

      @primary_key false
      schema "ch_nullables" do
        field(:string, Ch, type: "Nullable(String)")
        field(:fixed_string, Ch, type: "Nullable(FixedString(2))")
        field(:float, Ch, type: "Nullable(Float32)")
        field(:uuid, Ch, type: "Nullable(UUID)")
        field(:int, Ch, type: "Nullable(Int32)")
        field(:date, Ch, type: "Nullable(Date)")
        field(:date32, Ch, type: "Nullable(Date32)")
        field(:datetime, Ch, type: "Nullable(DateTime)")
        field(:datetime_utc, Ch, type: "Nullable(DateTime('UTC'))")
        field(:datetime64, Ch, type: "Nullable(DateTime64(6))")
        field(:datetime64_utc, Ch, type: "Nullable(DateTime64(6, 'UTC'))")
        field(:enum8, Ch, type: "Nullable(Enum8('hello' = 1, 'world' = 2))")
        field(:enum16, Ch, type: "Nullable(Enum16('hello' = 1, 'world' = 2))")
        field(:decimal, Ch, type: "Nullable(Decimal64(2))")
      end
    end

    test "insert_all" do
      uuid = Ecto.UUID.generate()
      today = Date.utc_today()
      naive_now = NaiveDateTime.utc_now()
      utc_now = DateTime.utc_now()

      assert {2, _} =
               insert_all(Nullables, [
                 _nils = Enum.map(Nullables.__schema__(:fields), fn field -> {field, nil} end),
                 [
                   string: "hello",
                   fixed_string: "AB",
                   float: 42.0,
                   uuid: uuid,
                   int: 42,
                   date: today,
                   date32: today,
                   datetime: NaiveDateTime.truncate(naive_now, :second),
                   datetime_utc: DateTime.truncate(utc_now, :second),
                   datetime64: naive_now,
                   datetime64_utc: utc_now,
                   enum8: "hello",
                   enum16: "world",
                   decimal: Decimal.new("13.37")
                 ]
               ])

      assert Nullables |> order_by([n], asc_nulls_first: n.string) |> all() |> unstruct() == [
               %{
                 date: nil,
                 date32: nil,
                 datetime: nil,
                 datetime64: nil,
                 datetime64_utc: nil,
                 datetime_utc: nil,
                 decimal: nil,
                 enum16: nil,
                 enum8: nil,
                 fixed_string: nil,
                 float: nil,
                 int: nil,
                 string: nil,
                 uuid: nil
               },
               %{
                 date: today,
                 date32: today,
                 datetime: NaiveDateTime.truncate(naive_now, :second),
                 datetime64: naive_now,
                 datetime64_utc: utc_now,
                 datetime_utc: DateTime.truncate(utc_now, :second),
                 decimal: Decimal.new("13.37"),
                 enum16: "world",
                 enum8: "hello",
                 fixed_string: "AB",
                 float: 42.0,
                 int: 42,
                 string: "hello",
                 uuid: uuid
               }
             ]
    end
  end

  describe "IP" do
    setup do
      query!("create table chips (v4 IPv4, v6 IPv6) engine Memory")
      on_exit(fn -> query!("truncate chips") end)
    end

    defmodule IPs do
      use Ecto.Schema

      @primary_key false
      schema "chips" do
        field(:v4, Ch, type: "IPv4")
        field(:v6, Ch, type: "IPv6")
      end
    end

    test "insert_all" do
      assert {2, _} =
               insert_all(IPs, [
                 [v4: nil, v6: nil],
                 [v4: {127, 0, 0, 1}, v6: {0, 0, 0, 0, 0, 0, 0, 1}]
                 # TODO
                 # [v4: "127.0.0.1", v6: "::1"]
               ])

      assert IPs |> order_by([i], i.v4) |> all() |> unstruct() == [
               %{v4: {0, 0, 0, 0}, v6: {0, 0, 0, 0, 0, 0, 0, 0}},
               %{v4: {127, 0, 0, 1}, v6: {0, 0, 0, 0, 0, 0, 0, 1}}
             ]
    end
  end

  describe "Geo" do
    setup do
      query!("""
      create table ch_geo (
        point Point,
        ring Ring,
        polygon Polygon,
        multipolygon MultiPolygon
      ) engine Memory
      """)

      on_exit(fn -> query!("truncate ch_geo") end)
    end

    defmodule Geo do
      use Ecto.Schema

      @primary_key false
      schema "ch_geo" do
        field(:point, Ch, type: "Point")
        field(:ring, Ch, type: "Ring")
        field(:polygon, Ch, type: "Polygon")
        field(:multipolygon, Ch, type: "MultiPolygon")
      end
    end

    test "insert_all" do
      assert {2, _} =
               insert_all(Geo, [
                 [point: nil, ring: nil, polygon: nil, multipolygon: nil],
                 [
                   point: {10, 10},
                   ring: [{0, 0}, {10, 0}, {10, 10}, {0, 10}],
                   polygon: [
                     [{20, 20}, {50, 20}, {50, 50}, {20, 50}],
                     [{30, 30}, {50, 50}, {50, 30}]
                   ],
                   multipolygon: [
                     [[{0, 0}, {10, 0}, {10, 10}, {0, 10}]],
                     [[{20, 20}, {50, 20}, {50, 50}, {20, 50}], [{30, 30}, {50, 50}, {50, 30}]]
                   ]
                 ]
               ])

      assert Geo |> order_by([g], g.point) |> all() |> unstruct() == [
               %{
                 point: {0.0, 0.0},
                 ring: [],
                 polygon: [],
                 multipolygon: []
               },
               %{
                 point: {10.0, 10.0},
                 ring: [{0.0, 0.0}, {10.0, 0.0}, {10.0, 10.0}, {0.0, 10.0}],
                 polygon: [
                   [{20.0, 20.0}, {50.0, 20.0}, {50.0, 50.0}, {20.0, 50.0}],
                   [{30.0, 30.0}, {50.0, 50.0}, {50.0, 30.0}]
                 ],
                 multipolygon: [
                   [[{0.0, 0.0}, {10.0, 0.0}, {10.0, 10.0}, {0.0, 10.0}]],
                   [
                     [{20.0, 20.0}, {50.0, 20.0}, {50.0, 50.0}, {20.0, 50.0}],
                     [{30.0, 30.0}, {50.0, 50.0}, {50.0, 30.0}]
                   ]
                 ]
               }
             ]
    end
  end

  defp unstruct([%schema{} | _] = structs) do
    Enum.map(structs, &Map.take(&1, schema.__schema__(:fields)))
  end
end
