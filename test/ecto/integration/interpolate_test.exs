defmodule Ecto.Integration.InterpolateTest do
  use Ecto.Integration.Case, async: true
  import Ecto.Query
  alias Ecto.Integration.TestRepo

  def all(query) do
    {sql, _params} = TestRepo.to_sql(:all, query, interpolate: true)
    %{sql: sql, rows: TestRepo.query!(sql, _no_params = []).rows}
  end

  def one(query) do
    assert %{rows: [[_true = 1]], sql: sql} = all(query)
    sql
  end

  describe "interpolate and exec" do
    test "in WHERE" do
      assert all(from n in fragment("numbers(1)"), where: n.number == ^0, select: n.number) == %{
               rows: [[0]],
               sql: ~s{SELECT f0."number" FROM numbers(1) AS f0 WHERE (f0."number" = 0)}
             }
    end

    test "in HAVING" do
      assert all(from n in fragment("numbers(1)"), having: n.number == ^0, select: n.number) ==
               %{
                 rows: [[0]],
                 sql: ~s{SELECT f0."number" FROM numbers(1) AS f0 HAVING (f0."number" = 0)}
               }
    end

    test "in fragment" do
      assert all(from n in fragment("numbers(?)", ^1), select: n.number) == %{
               rows: [[0]],
               sql: ~s{SELECT f0."number" FROM numbers(1) AS f0}
             }
    end

    @tag :skip
    test "in JOIN"
    @tag :skip
    test "in WINDOW"
    @tag :skip
    test "in SELECT"
    @tag :skip
    test "in WITH"

    test "in subquery" do
      assert all(
               from n in fragment("numbers(2)"),
                 where:
                   n.number in subquery(from n in fragment("numbers(2)"), select: n.number + ^1),
                 select: n.number
             ) == %{
               rows: [[1]],
               sql:
                 ~s{SELECT f0."number" FROM numbers(2) AS f0 WHERE (f0."number" IN (SELECT sf0."number" + 1 FROM numbers(2) AS sf0))}
             }
    end

    test "in OFFSET and LIMIT" do
      assert all(from n in fragment("numbers(2)"), offset: ^1, limit: ^1, select: n.number) == %{
               rows: [[1]],
               sql: ~s{SELECT f0."number" FROM numbers(2) AS f0 LIMIT 1 OFFSET 1}
             }
    end

    @tag :skip
    test "in GROUP BY"
    @tag :skip
    test "in ORDER BY"

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
                   fragment("toDateTime('1970-01-01 00:00:00')") == ^~N[1970-01-01 00:00:00.000]
             ) ==
               "SELECT toDateTime('1970-01-01 00:00:00') = '1970-01-01 00:00:00.000'::DateTime64(3) FROM system.one AS f0"

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

    @tag :skip
    test "with enums"

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

    @tag :skip
    test "with uuid"
    @tag :skip
    test "with ipv4"
    @tag :skip
    test "with ipv6"

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

    @tag :skip
    test "with tuple"

    test "with map" do
      assert one(from fragment("system.one"), select: 1 == fragment("?[1]", ^%{1 => 1})) ==
               "SELECT 1 = map(1,1)[1] FROM system.one AS f0"

      assert one(
               from fragment("system.one"),
                 select: 1 == fragment("?[?]", ^%{"a" => 0, "b" => 1}, ^"b")
             ) == "SELECT 1 = map('a',0,'b',1)['b'] FROM system.one AS f0"
    end
  end
end
