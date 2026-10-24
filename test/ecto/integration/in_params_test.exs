defmodule Ecto.Integration.InParamsTest do
  use Ecto.Integration.Case, async: true

  import Ecto.Query
  alias Ecto.Integration.TestRepo

  test "IN parameters preserve large integers mixed with floats" do
    # 2^53 + 1 rounds down if the list is coerced to Float64.
    values = [9_007_199_254_740_993, 0.5]

    for values <- [values, Enum.reverse(values)] do
      query =
        from fragment("system.one"),
          select:
            {fragment("toInt64(9007199254740993)") in ^values,
             fragment("toInt64(9007199254740992)") in ^values}

      assert TestRepo.all(query) == [{1, 0}]
    end
  end

  @tag :time
  test "IN parameters support Time values" do
    values = [~T[12:00:00], ~T[13:00:00]]

    query =
      from fragment("system.one"),
        select:
          {fragment("CAST('12:00:00' AS Time)") in ^values,
           fragment("CAST('14:00:00' AS Time)") in ^values}

    assert TestRepo.all(query) == [{1, 0}]
  end

  @tag :time
  test "IN parameters preserve Time64 precision" do
    values = [~T[12:00:00.123456], ~T[13:00:00.654321]]

    query =
      from fragment("system.one"),
        select:
          {fragment("toTime64('12:00:00.123456',6)") in ^values,
           fragment("toTime64('12:00:00.123455',6)") in ^values}

    assert TestRepo.all(query) == [{1, 0}]
  end

  test "IN parameters preserve dates beyond the DateTime range when mixed with datetimes" do
    # DateTime ends on 2106-02-07, although Date can represent the rest of 2106.
    values = [~D[2106-12-01], ~N[2020-01-01 00:00:00]]

    for values <- [values, Enum.reverse(values)] do
      query =
        from fragment("system.one"),
          select:
            {fragment("toDate32('2106-12-01')") in ^values,
             fragment("toDate32('2106-02-07')") in ^values}

      assert TestRepo.all(query) == [{1, 0}]
    end
  end

  test "IN parameters preserve mixed dates and datetimes in every order" do
    date = ~D[2200-01-01]
    whole = ~N[2020-01-01 00:00:00]
    fractional = ~N[2020-01-01 00:00:00.123]

    for values <- [
          [date, whole, fractional],
          [date, fractional, whole],
          [whole, date, fractional],
          [whole, fractional, date],
          [fractional, date, whole],
          [fractional, whole, date]
        ] do
      query =
        from fragment("system.one"),
          select:
            {fragment("toDateTime64('2020-01-01 00:00:00',3)") in ^values,
             fragment("toDateTime64('2020-01-01 00:00:00.123',3)") in ^values,
             fragment("toDateTime64('2020-01-01 00:00:00.124',3)") in ^values}

      assert TestRepo.all(query) == [{1, 1, 0}]
    end
  end

  test "IN parameters accept mixed decimals and numbers" do
    for number <- [2, 2.0],
        values <- [[Decimal.new("1.0"), number], [number, Decimal.new("1.0")]] do
      query =
        from fragment("system.one"),
          select:
            {fragment("toDecimal64(1,1)") in ^values, fragment("toDecimal64(2,1)") in ^values,
             fragment("toDecimal64('1.5',1)") in ^values}

      assert TestRepo.all(query) == [{1, 1, 0}]
    end
  end

  test "IN parameters accept dates outside the DateTime range with whole-second datetimes" do
    values = [~D[2200-01-01], ~N[2020-01-01 00:00:00]]

    for values <- [values, Enum.reverse(values)] do
      query =
        from fragment("system.one"),
          select:
            {fragment("toDateTime64('2020-01-01 00:00:00',0)") in ^values,
             fragment("toDateTime64('2021-01-01 00:00:00',0)") in ^values}

      assert TestRepo.all(query) == [{1, 0}]
    end
  end

  test "IN parameters preserve decimals that cannot share one precision and scale" do
    large = Decimal.new("1E75")
    small = Decimal.new("1E-75")
    absent = Decimal.new("2E-75")
    values = [large, small]

    for values <- [values, Enum.reverse(values)] do
      query =
        from fragment("system.one"),
          select:
            {fragment("?", ^large) in ^values, fragment("?", ^small) in ^values,
             fragment("?", ^absent) in ^values}

      assert TestRepo.all(query) == [{1, 1, 0}]
    end
  end

  test "IN parameters preserve integers that cannot share one signed or unsigned type" do
    large = Integer.pow(2, 255)
    values = [-1, large]

    for values <- [values, Enum.reverse(values)] do
      query =
        from fragment("system.one"),
          select: {-1 in ^values, fragment("?", ^large) in ^values, 0 in ^values}

      assert TestRepo.all(query) == [{1, 1, 0}]
    end
  end

  test "IN parameters accept enum labels and numeric values together" do
    for enum_type <- ["Enum8('foo'=1,'bar'=2,'baz'=3)", "Enum16('foo'=1,'bar'=2,'baz'=3)"],
        type_name <- [enum_type, "Nullable(#{enum_type})"] do
      ch_type = Ecto.ParameterizedType.init(Ch, type: type_name)

      source =
        from fragment("system.one"),
          select: %{
            value: type(fragment("CAST(arrayJoin(['foo','bar','baz']), ?)", ^type_name), ^ch_type)
          }

      for values <- [["foo", "bar"], [1, 2], ["foo", 2], [2, "foo"]] do
        query =
          from s in subquery(source),
            where: s.value in ^values,
            select: s.value

        assert Enum.sort(TestRepo.all(query)) == ["bar", "foo"],
               "#{type_name} with #{inspect(values)}"
      end
    end
  end

  test "IN parameters accept arrays of enum labels and numeric values together" do
    for enum_type <- ["Enum8('foo'=1,'bar'=2,'baz'=3)", "Enum16('foo'=1,'bar'=2,'baz'=3)"] do
      type_name = "Array(#{enum_type})"
      ch_type = Ecto.ParameterizedType.init(Ch, type: type_name)

      source =
        from fragment("system.one"),
          select: %{
            value:
              type(fragment("CAST([arrayJoin(['foo','bar','baz'])], ?)", ^type_name), ^ch_type)
          }

      for values <- [[["foo"], ["bar"]], [[1], [2]], [["foo"], [2]], [[2], ["foo"]]] do
        query =
          from s in subquery(source),
            where: s.value in ^values,
            select: s.value

        assert Enum.sort(TestRepo.all(query)) == [["bar"], ["foo"]],
               "Array(#{enum_type}) with #{inspect(values)}"
      end
    end
  end

  @tag :variant
  test "IN parameters accept values from different Variant alternatives" do
    ch_type = Ecto.ParameterizedType.init(Ch, type: "Variant(String,Int64)")

    source =
      from fragment("system.one"),
        select: %{
          value:
            type(
              fragment("""
              arrayJoin([\
              CAST('foo' AS Variant(String,Int64)),\
              CAST(toInt64(2) AS Variant(String,Int64)),\
              CAST('bar' AS Variant(String,Int64)),\
              CAST(toInt64(3) AS Variant(String,Int64))])\
              """),
              ^ch_type
            )
        }

    for values <- [["foo", 2], [2, "foo"]] do
      query =
        from s in subquery(source),
          where: s.value in ^values,
          select: s.value

      assert Enum.sort(TestRepo.all(query)) == [2, "foo"]
    end
  end

  @tag :variant
  test "IN parameters preserve numeric Variant alternatives even without precision loss" do
    values = [1, 2.5]

    for values <- [values, Enum.reverse(values)] do
      query =
        from fragment("system.one"),
          select:
            {fragment("CAST(toInt64(1) AS Variant(Int64,Float64))") in ^values,
             fragment("CAST(toFloat64(1) AS Variant(Int64,Float64))") in ^values,
             fragment("CAST(toFloat64(2.5) AS Variant(Int64,Float64))") in ^values}

      assert TestRepo.all(query, settings: [allow_suspicious_variant_types: 1]) == [{1, 0, 1}]
    end
  end
end
