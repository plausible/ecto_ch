defmodule Ecto.Integration.QueryParamTypeTest do
  use Ecto.Integration.Case
  import Ecto.Query
  alias Ecto.Integration.TestRepo

  test "String" do
    q =
      from v in fragment("values ('s String', ('one'), ('two'), ('three'))"),
        select: v.s

    assert TestRepo.one!(where(q, s: ^"one")) == "one"
    assert TestRepo.one!(where(q, s: ^"two")) == "two"
    assert TestRepo.one!(where(q, s: ^"three")) == "three"
  end

  # https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
  test "UInt64" do
    q =
      from v in fragment(
             "values ('u UInt64', (0), (9223372036854775807), (18446744073709551615))"
           ),
           select: v.u

    assert TestRepo.one!(where(q, u: ^0)) == 0
    assert TestRepo.one!(where(q, u: ^0x7FFFFFFFFFFFFFFF)) == 9_223_372_036_854_775_807
    assert TestRepo.one!(where(q, [v], v.u > ^(0x7FFFFFFFFFFFFFFF + 1))) == 0xFFFFFFFFFFFFFFFF
    assert TestRepo.one!(where(q, u: ^0xFFFFFFFFFFFFFFFF)) == 18_446_744_073_709_551_615
  end

  # https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
  test "Int64" do
    q =
      from v in fragment("values ('i Int64', (-9223372036854775808), (0), (9223372036854775807))"),
        select: v.i

    assert TestRepo.one!(where(q, i: ^(-0x8000000000000000))) == -9_223_372_036_854_775_808
    assert TestRepo.one!(where(q, i: ^0)) == 0
    assert TestRepo.one!(where(q, i: ^0x7FFFFFFFFFFFFFFF)) == 9_223_372_036_854_775_807
  end

  # https://clickhouse.com/docs/en/sql-reference/data-types/float
  test "Float64" do
    q =
      from v in fragment("values ('f Float64', (0), (-500279.563), (500279.56300000014))"),
        select: v.f

    assert TestRepo.one!(where(q, f: ^0.0)) == 0
    assert TestRepo.one!(where(q, f: ^(-500_279.563))) == -500_279.563
    assert TestRepo.one!(where(q, f: ^500_279.56300000014)) == 500_279.56300000014
  end

  # https://clickhouse.com/docs/en/sql-reference/data-types/boolean
  test "Bool" do
    q =
      from v in fragment("values ('b Bool', (false), (true))"),
        select: v.b

    assert TestRepo.one!(where(q, b: ^false)) == false
    assert TestRepo.one!(where(q, b: ^true)) == true
  end

  # https://clickhouse.com/docs/en/sql-reference/data-types/datetime
  test "DateTime" do
    q =
      from v in fragment(
             # unix(~N[2023-12-16 08:55:47]) = 1702716947
             "values ('d DateTime', ('1970-01-01 00:00:00'), (1702716947), ('2106-02-07 06:28:15'))"
           ),
           select: v.d

    assert TestRepo.one!(where(q, d: ^~N[1970-01-01 00:00:00])) == ~N[1970-01-01 00:00:00]
    assert TestRepo.one!(where(q, d: ^~N[1970-01-01 00:00:00.000])) == ~N[1970-01-01 00:00:00]
    assert TestRepo.one!(where(q, d: ^~U[1970-01-01 00:00:00Z])) == ~N[1970-01-01 00:00:00]
    # TODO
    # ** (Ch.Error) Code: 457. DB::Exception: Value 0.0 cannot be parsed as DateTime64 for query parameter '$0' because it isn't parsed completely: only 1 of 3 bytes was parsed: 0. (BAD_QUERY_PARAMETER) (version 23.3.7.5 (official build))
    # assert TestRepo.one!(where(q, d: ^~U[1970-01-01 00:00:00.000Z])) == ~N[1970-01-01 00:00:00]

    assert TestRepo.one!(where(q, d: ^1_702_716_947)) == ~N[2023-12-16 08:55:47]
    assert TestRepo.one!(where(q, d: ^1_702_716_947.0000)) == ~N[2023-12-16 08:55:47]
    assert TestRepo.one!(where(q, d: ^~N[2023-12-16 08:55:47])) == ~N[2023-12-16 08:55:47]
    assert TestRepo.one!(where(q, d: ^~U[2023-12-16 08:55:47Z])) == ~N[2023-12-16 08:55:47]
    assert TestRepo.one!(where(q, d: ^~U[2023-12-16 08:55:47.000Z])) == ~N[2023-12-16 08:55:47]

    assert TestRepo.one!(where(q, d: ^~N[2106-02-07 06:28:15])) == ~N[2106-02-07 06:28:15]
    assert TestRepo.one!(where(q, d: ^~N[2106-02-07 06:28:15.000000])) == ~N[2106-02-07 06:28:15]
    assert TestRepo.one!(where(q, d: ^~U[2106-02-07 06:28:15Z])) == ~N[2106-02-07 06:28:15]
    assert TestRepo.one!(where(q, d: ^~U[2106-02-07 06:28:15.000000Z])) == ~N[2106-02-07 06:28:15]

    assert TestRepo.one!(where(q, [v], v.d < ^~N[1970-01-01 00:00:00.123456])) ==
             ~N[1970-01-01 00:00:00]

    # TODO
    # ** (Ch.Error) Code: 457. DB::Exception: Value 0.123456 cannot be parsed as DateTime64 for query parameter '$0' because it isn't parsed completely: only 1 of 8 bytes was parsed: 0. (BAD_QUERY_PARAMETER) (version 23.3.7.5 (official build))
    # assert TestRepo.one!(where(q, [v], v.d < ^~U[1970-01-01 00:00:00.123456Z])) ==
    #          ~N[1970-01-01 00:00:00]

    assert TestRepo.all(where(q, [v], v.d > ^~N[1970-01-01 00:00:00.123456])) ==
             [~N[2023-12-16 08:55:47], ~N[2106-02-07 06:28:15]]

    # TODO
    # ** (Ch.Error) Code: 457. DB::Exception: Value 0.123456 cannot be parsed as DateTime64 for query parameter '$0' because it isn't parsed completely: only 1 of 8 bytes was parsed: 0. (BAD_QUERY_PARAMETER) (version 23.3.7.5 (official build))
    # assert TestRepo.all(where(q, [v], v.d > ^~U[1970-01-01 00:00:00.123456Z])) ==
    #  [~N[2023-12-16 08:55:47], ~N[2106-02-07 06:28:15]]

    assert TestRepo.all(where(q, [v], v.d < ^1_702_716_947.123)) ==
             [~N[1970-01-01 00:00:00], ~N[2023-12-16 08:55:47]]
  end
end
