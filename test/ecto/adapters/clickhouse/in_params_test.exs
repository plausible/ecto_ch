defmodule Ecto.Adapters.ClickHouse.InParamsTest do
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
end
