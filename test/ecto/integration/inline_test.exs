defmodule Ecto.Integration.InlineSQLTest do
  use Ecto.Integration.Case, async: true
  import Ecto.Query
  alias Ecto.Integration.TestRepo

  def all(query) do
    sql = TestRepo.to_inline_sql(:all, query)
    %{sql: sql, rows: TestRepo.query!(sql, _no_params = []).rows}
  end

  def one(query) do
    assert %{rows: [[_true = 1]], sql: sql} = all(query)
    sql
  end

  describe "to_inline_sql/2" do
    test "everything query" do
      cte1 =
        "schema1"
        |> select([m], %{id: m.id, smth: ^true})
        |> where([], fragment("?", ^1))

      union =
        "schema1"
        |> select([m], {m.id, ^true})
        |> where([], fragment("?", ^5))

      union_all =
        "schema2"
        |> select([m], {m.id, ^false})
        |> where([], fragment("?", ^6))

      query =
        "schema"
        |> with_cte("cte1", as: ^cte1)
        |> with_cte("cte2", as: fragment("SELECT * FROM schema WHERE ?", ^2))
        |> select([m], {m.id, ^0})
        |> join(:inner, [], "schema2", on: fragment("?", ^true))
        |> join(:inner, [], "schema2", on: fragment("?", ^false))
        |> where([], fragment("?", ^true))
        |> where([], fragment("?", ^false))
        |> having([], fragment("?", ^true))
        |> having([], fragment("?", ^false))
        |> group_by([], fragment("?", ^3))
        |> group_by([], fragment("?", ^4))
        |> union(^union)
        |> union_all(^union_all)
        |> order_by([], fragment("?", ^7))
        |> limit([], ^8)
        |> offset([], ^9)

      assert TestRepo.to_inline_sql(:all, query) ==
               """
               WITH \
               "cte1" AS (\
               SELECT ss0."id" AS "id",true AS "smth" FROM "schema1" AS ss0 \
               WHERE (1)\
               ),\
               "cte2" AS (\
               SELECT * FROM schema WHERE 2\
               ) \
               SELECT s0."id",0 FROM "schema" AS s0 \
               INNER JOIN "schema2" AS s1 ON true \
               INNER JOIN "schema2" AS s2 ON false \
               WHERE (true) AND (false) \
               GROUP BY 3,4 \
               HAVING (true) AND (false) \
               ORDER BY 7 \
               LIMIT 8 \
               OFFSET 9 \
               UNION \
               (SELECT s0."id",true FROM "schema1" AS s0 \
               WHERE (5)) \
               UNION ALL \
               (SELECT s0."id",false FROM "schema2" AS s0 \
               WHERE (6))\
               """
    end

    test "delete all" do
      assert TestRepo.to_inline_sql(:delete_all, from(e in "schema", where: e.x == ^123)) ==
               ~s[DELETE FROM "schema" WHERE ("x" = 123)]
    end
  end

  describe "inline and exec" do
    # https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
    test "with integers" do
      assert one(from fragment("system.one"), select: 1 == ^1) ==
               "SELECT 1 = 1 FROM system.one AS f0"
    end

    # https://clickhouse.com/docs/en/sql-reference/data-types/float
    test "with floats" do
      assert one(from fragment("system.one"), select: 0.1 == ^0.1) ==
               "SELECT 0.1 = 0.1 FROM system.one AS f0"
    end

    # https://clickhouse.com/docs/en/sql-reference/data-types/decimal
    test "with decimals" do
      assert one(
               from fragment("system.one"),
                 select: fragment("toDecimal32(1,4)") == ^Decimal.new("1.0")
             ) ==
               "SELECT toDecimal32(1,4) = 1.0 FROM system.one AS f0"
    end

    # https://clickhouse.com/docs/en/sql-reference/data-types/string
    test "with strings" do
      assert one(from fragment("system.one"), select: "asdf" == ^"asdf") ==
               "SELECT 'asdf' = 'asdf' FROM system.one AS f0"

      assert one(from fragment("system.one"), select: ~s{\\} == ^"\\") ==
               "SELECT '\\\\' = '\\\\' FROM system.one AS f0"

      assert one(from fragment("system.one"), select: "'" == ^"'") ==
               "SELECT '''' = '''' FROM system.one AS f0"
    end

    # https://clickhouse.com/docs/en/sql-reference/data-types/date
    # https://clickhouse.com/docs/en/sql-reference/data-types/date32
    test "with dates" do
      assert one(from fragment("system.one"), select: fragment("toDate(0)") == ^~D[1970-01-01]) ==
               "SELECT toDate(0) = '1970-01-01'::date FROM system.one AS f0"

      # can apply toDate
      assert one(
               from fragment("system.one"),
                 select: fragment("toDate(?)", ^~D[1970-01-01]) == ^~D[1970-01-01]
             ) ==
               "SELECT toDate('1970-01-01'::date) = '1970-01-01'::date FROM system.one AS f0"

      # can apply functions
      assert one(
               from fragment("system.one"),
                 select: fragment("toMonday(?)", ^~D[2000-01-01]) == ^~D[1999-12-27]
             ) ==
               "SELECT toMonday('2000-01-01'::date) = '1999-12-27'::date FROM system.one AS f0"

      # uses date32 when date is in far future
      assert one(
               from fragment("system.one"), select: fragment("toDate32(99999)") == ^~D[2243-10-16]
             ) ==
               "SELECT toDate32(99999) = '2243-10-16'::date32 FROM system.one AS f0"
    end

    test "with datetimes" do
      assert one(
               from fragment("system.one"),
                 select: fragment("toDateTime(0)") == ^~U[1970-01-01 00:00:00Z]
             ) ==
               "SELECT toDateTime(0) = '1970-01-01 00:00:00'::DateTime('Etc/UTC') FROM system.one AS f0"

      # can apply toDateTime
      assert one(
               from fragment("system.one"),
                 select:
                   fragment("toDateTime(?)", ^~U[2024-04-13 11:00:06Z]) ==
                     ^~U[2024-04-13 11:00:06Z]
             ) ==
               "SELECT toDateTime('2024-04-13 11:00:06'::DateTime('Etc/UTC')) = '2024-04-13 11:00:06'::DateTime('Etc/UTC') FROM system.one AS f0"

      assert one(
               from fragment("system.one"),
                 select:
                   fragment("toDateTime(?)", ^~U[2024-04-13 11:00:06.935753Z]) ==
                     ^~U[2024-04-13 11:00:06Z]
             ) ==
               "SELECT toDateTime('2024-04-13 11:00:06.935753'::DateTime64(6,'Etc/UTC')) = '2024-04-13 11:00:06'::DateTime('Etc/UTC') FROM system.one AS f0"

      # uses datetime64 when datetime has microseconds
      assert one(
               from fragment("system.one"),
                 select: fragment("toDateTime(0)") == ^~U[1970-01-01 00:00:00.000Z]
             ) ==
               "SELECT toDateTime(0) = '1970-01-01 00:00:00.000'::DateTime64(3,'Etc/UTC') FROM system.one AS f0"

      # can apply toDateTime64
      assert one(
               from fragment("system.one"),
                 select:
                   fragment("toDateTime64(?,3)", ^~U[2024-04-13 11:00:06.935753Z]) ==
                     ^~U[2024-04-13 11:00:06.935Z]
             ) ==
               "SELECT toDateTime64('2024-04-13 11:00:06.935753'::DateTime64(6,'Etc/UTC'),3) = '2024-04-13 11:00:06.935'::DateTime64(3,'Etc/UTC') FROM system.one AS f0"

      # can apply functions
      assert one(
               from fragment("system.one"),
                 select:
                   fragment("toDate(toTimeZone(?,'Asia/Taipei'))", ^~U[2024-04-13 11:00:06Z]) ==
                     "2024-04-13"
             ) ==
               "SELECT toDate(toTimeZone('2024-04-13 11:00:06'::DateTime('Etc/UTC'),'Asia/Taipei')) = '2024-04-13' FROM system.one AS f0"
    end

    test "with naive datetimes" do
      assert one(
               from fragment("system.one"),
                 select: fragment("toDateTime('1970-01-01 00:00:00')") == ^~N[1970-01-01 00:00:00]
             ) ==
               "SELECT toDateTime('1970-01-01 00:00:00') = '1970-01-01 00:00:00'::datetime FROM system.one AS f0"

      # can apply toDateTime
      assert one(
               from fragment("system.one"),
                 select:
                   fragment("toDateTime(?)", ^~N[2024-04-13 11:00:06]) ==
                     ^~N[2024-04-13 11:00:06]
             ) ==
               "SELECT toDateTime('2024-04-13 11:00:06'::datetime) = '2024-04-13 11:00:06'::datetime FROM system.one AS f0"

      assert one(
               from fragment("system.one"),
                 select:
                   fragment("toDateTime(?)", ^~N[2024-04-13 11:00:06.935753]) ==
                     ^~N[2024-04-13 11:00:06]
             ) ==
               "SELECT toDateTime('2024-04-13 11:00:06.935753'::DateTime64(6)) = '2024-04-13 11:00:06'::datetime FROM system.one AS f0"

      # uses datetime64 when datetime has microseconds
      assert one(
               from fragment("system.one"),
                 select:
                   fragment("toDateTime('1970-01-01 10:00:00')") == ^~N[1970-01-01 10:00:00.000]
             ) ==
               "SELECT toDateTime('1970-01-01 10:00:00') = '1970-01-01 10:00:00.000'::DateTime64(3) FROM system.one AS f0"

      # can apply toDateTime64
      assert one(
               from fragment("system.one"),
                 select:
                   fragment("toDateTime64(?,3)", ^~N[2024-04-13 11:00:06.935753]) ==
                     ^~N[2024-04-13 11:00:06.935]
             ) ==
               "SELECT toDateTime64('2024-04-13 11:00:06.935753'::DateTime64(6),3) = '2024-04-13 11:00:06.935'::DateTime64(3) FROM system.one AS f0"

      # can apply functions
      assert one(
               from fragment("system.one"),
                 select:
                   fragment("toDate(toTimeZone(?,'Asia/Taipei'))", ^~N[2024-04-13 11:00:06]) ==
                     "2024-04-13"
             ) ==
               "SELECT toDate(toTimeZone('2024-04-13 11:00:06'::datetime,'Asia/Taipei')) = '2024-04-13' FROM system.one AS f0"
    end

    test "with enums" do
      assert one(
               from fragment("system.one"),
                 select: fragment("CAST('a' AS Enum('a' = 1, 'b' = 2))") == ^"a"
             ) == "SELECT CAST('a' AS Enum('a' = 1, 'b' = 2)) = 'a' FROM system.one AS f0"

      assert one(
               from fragment("system.one"),
                 select: fragment("CAST('a' AS Enum('a' = 1, 'b' = 2))") == ^1
             ) == "SELECT CAST('a' AS Enum('a' = 1, 'b' = 2)) = 1 FROM system.one AS f0"
    end

    test "with booleans" do
      assert one(from fragment("system.one"), select: true == ^true) ==
               "SELECT 1 = true FROM system.one AS f0"

      assert one(from fragment("system.one"), select: false == ^false) ==
               "SELECT 0 = false FROM system.one AS f0"

      assert one(from fragment("system.one"), select: fragment("true") == ^true) ==
               "SELECT true = true FROM system.one AS f0"

      assert one(from fragment("system.one"), select: fragment("false") == ^false) ==
               "SELECT false = false FROM system.one AS f0"

      assert one(from fragment("system.one"), select: fragment("?", 1) == ^true) ==
               "SELECT 1 = true FROM system.one AS f0"

      assert one(from fragment("system.one"), select: fragment("?", 0) == ^false) ==
               "SELECT 0 = false FROM system.one AS f0"
    end

    test "with uuid" do
      uuid = "601d74e4-a8d3-4b6e-8365-eddb4c893327"

      assert one(from fragment("system.one"), select: type(^uuid, Ecto.UUID) == ^uuid) ==
               "SELECT CAST('601d74e4-a8d3-4b6e-8365-eddb4c893327' AS UUID) = '601d74e4-a8d3-4b6e-8365-eddb4c893327' FROM system.one AS f0"

      assert <<uuid_raw::16-bytes>> = Ecto.UUID.dump!(uuid)

      assert one(from fragment("system.one"), select: type(^uuid_raw, Ecto.UUID) == ^uuid) ==
               "SELECT CAST('601d74e4-a8d3-4b6e-8365-eddb4c893327' AS UUID) = '601d74e4-a8d3-4b6e-8365-eddb4c893327' FROM system.one AS f0"

      assert one(from fragment("system.one"), select: type(^uuid, Ecto.UUID) == ^uuid_raw) ==
               "SELECT CAST('601d74e4-a8d3-4b6e-8365-eddb4c893327' AS UUID) = '601d74e4-a8d3-4b6e-8365-eddb4c893327' FROM system.one AS f0"

      assert one(from fragment("system.one"), select: ^uuid_raw == ^uuid_raw) ==
               "SELECT '`\x1Dt\xE4\xA8\xD3Kn\x83e\xED\xDBL\x893''' = '`\x1Dt\xE4\xA8\xD3Kn\x83e\xED\xDBL\x893''' FROM system.one AS f0"
    end

    test "with ipv4" do
      assert one(
               from fragment("system.one"),
                 select: fragment("'116.253.40.133'::IPv4") == ^"116.253.40.133"
             ) == "SELECT '116.253.40.133'::IPv4 = '116.253.40.133' FROM system.one AS f0"
    end

    test "with ipv6" do
      assert one(
               from fragment("system.one"),
                 select: fragment("'2a02:aa08:e000:3100::2'::IPv6") == ^"2a02:aa08:e000:3100::2"
             ) ==
               "SELECT '2a02:aa08:e000:3100::2'::IPv6 = '2a02:aa08:e000:3100::2' FROM system.one AS f0"
    end

    test "with array" do
      assert one(from fragment("system.one"), select: [] == ^[]) ==
               "SELECT [] = [] FROM system.one AS f0"

      assert one(from fragment("system.one"), select: [1] == ^[1]) ==
               "SELECT [1] = [1] FROM system.one AS f0"

      assert one(from fragment("system.one"), select: 1 == fragment("?[1]", ^[1])) ==
               "SELECT 1 = [1][1] FROM system.one AS f0"

      assert one(from fragment("system.one"), select: [1, 2, 3] == ^[1, 2, 3]) ==
               "SELECT [1,2,3] = [1,2,3] FROM system.one AS f0"

      assert one(from fragment("system.one"), select: ["a", "b", "c"] == ^["a", "b", "c"]) ==
               "SELECT ['a','b','c'] = ['a','b','c'] FROM system.one AS f0"

      assert one(
               from fragment("system.one"),
                 select: [fragment("toDate('2023-01-01')"), nil] == ^[~D[2023-01-01], nil]
             ) ==
               "SELECT [toDate('2023-01-01'),NULL] = ['2023-01-01'::date,NULL] FROM system.one AS f0"
    end

    test "with tuple" do
      assert one(from fragment("system.one"), select: fragment("tuple(1, 'a')") == ^{1, "a"}) ==
               "SELECT tuple(1, 'a') = (1,'a') FROM system.one AS f0"

      assert one(from fragment("system.one"), select: 1 == fragment("?.1", ^{1, nil})) ==
               "SELECT 1 = (1,NULL).1 FROM system.one AS f0"
    end

    test "with map" do
      assert one(from fragment("system.one"), select: 1 == fragment("?[1]", ^%{1 => 1})) ==
               "SELECT 1 = map(1,1)[1] FROM system.one AS f0"

      assert one(
               from fragment("system.one"),
                 select: 1 == fragment("?[?]", ^%{"a" => 0, "b" => 1}, ^"b")
             ) == "SELECT 1 = map('a',0,'b',1)['b'] FROM system.one AS f0"
    end

    # https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
    # https://github.com/plausible/ecto_ch/issues/187
    test "with large integers" do
      # Int128
      assert one(
               from f in fragment("system.one"),
                 select:
                   -170_141_183_460_469_231_731_687_303_715_884_105_728 ==
                     ^(-170_141_183_460_469_231_731_687_303_715_884_105_728)
             ) ==
               "SELECT -170141183460469231731687303715884105728 = -170141183460469231731687303715884105728 FROM system.one AS f0"

      assert one(
               from f in fragment("system.one"),
                 select:
                   170_141_183_460_469_231_731_687_303_715_884_105_727 ==
                     ^170_141_183_460_469_231_731_687_303_715_884_105_727
             ) ==
               "SELECT 170141183460469231731687303715884105727 = 170141183460469231731687303715884105727 FROM system.one AS f0"

      # Int256
      assert one(
               from f in fragment("system.one"),
                 select:
                   -57_896_044_618_658_097_711_785_492_504_343_953_926_634_992_332_820_282_019_728_792_003_956_564_819_968 ==
                     ^(-57_896_044_618_658_097_711_785_492_504_343_953_926_634_992_332_820_282_019_728_792_003_956_564_819_968)
             ) ==
               "SELECT -57896044618658097711785492504343953926634992332820282019728792003956564819968 = -57896044618658097711785492504343953926634992332820282019728792003956564819968 FROM system.one AS f0"

      assert one(
               from f in fragment("system.one"),
                 select:
                   57_896_044_618_658_097_711_785_492_504_343_953_926_634_992_332_820_282_019_728_792_003_956_564_819_967 ==
                     ^57_896_044_618_658_097_711_785_492_504_343_953_926_634_992_332_820_282_019_728_792_003_956_564_819_967
             ) ==
               "SELECT 57896044618658097711785492504343953926634992332820282019728792003956564819967 = 57896044618658097711785492504343953926634992332820282019728792003956564819967 FROM system.one AS f0"

      # UInt256
      assert one(
               from f in fragment("system.one"),
                 select:
                   115_792_089_237_316_195_423_570_985_008_687_907_853_269_984_665_640_564_039_457_584_007_913_129_639_935 ==
                     ^115_792_089_237_316_195_423_570_985_008_687_907_853_269_984_665_640_564_039_457_584_007_913_129_639_935
             ) ==
               "SELECT 115792089237316195423570985008687907853269984665640564039457584007913129639935 = 115792089237316195423570985008687907853269984665640564039457584007913129639935 FROM system.one AS f0"
    end
  end
end
