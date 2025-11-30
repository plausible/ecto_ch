defmodule Ecto.Integration.ClickHouseJoinsTest do
  use Ecto.Integration.Case, async: true
  import Ecto.Query
  alias Ecto.Integration.TestRepo

  # here we are testing some of the custom ClickHouse join types
  # - https://clickhouse.com/docs/en/sql-reference/statements/select/join
  # - https://clickhouse.com/blog/clickhouse-fully-supports-joins-part1

  # https://clickhouse.com/docs/en/sql-reference/statements/select/array-join#basic-array-join-examples
  @tag :capture_log
  test "array-join" do
    TestRepo.query!("CREATE TABLE arrays_test(s String, arr Array(UInt8)) ENGINE = Memory")
    on_exit(fn -> TestRepo.query!("DROP TABLE arrays_test") end)

    TestRepo.query!(
      "INSERT INTO arrays_test VALUES ('Hello', [1,2]), ('World', [3,4,5]), ('Goodbye', [])"
    )

    # SELECT s, arr FROM arrays_test ARRAY JOIN arr
    assert TestRepo.all(
             from t in "arrays_test",
               join: a in "arr",
               on: true,
               hints: "ARRAY",
               select: [t.s, fragment("?", a)]
           ) == [
             ["Hello", 1],
             ["Hello", 2],
             ["World", 3],
             ["World", 4],
             ["World", 5]
           ]

    # SELECT s, arr FROM arrays_test LEFT ARRAY JOIN arr;
    assert TestRepo.all(
             from t in "arrays_test",
               left_join: a in "arr",
               hints: "ARRAY",
               on: true,
               select: [t.s, fragment("?", a)]
           ) == [
             ["Hello", 1],
             ["Hello", 2],
             ["World", 3],
             ["World", 4],
             ["World", 5],
             ["Goodbye", 0]
           ]

    # SELECT s, arr, a FROM arrays_test ARRAY JOIN arr AS a;
    assert TestRepo.all(
             from t in "arrays_test",
               join: a in "arr",
               hints: "ARRAY",
               on: true,
               select: [t.s, fragment("arr"), fragment("?", a)]
           ) == [
             ["Hello", [1, 2], 1],
             ["Hello", [1, 2], 2],
             ["World", [3, 4, 5], 3],
             ["World", [3, 4, 5], 4],
             ["World", [3, 4, 5], 5]
           ]

    # SELECT s, arr_external FROM arrays_test ARRAY JOIN [1, 2, 3] AS arr_external;
    assert TestRepo.all(
             from t in "arrays_test",
               join: a in fragment("?", [1, 2, 3]),
               hints: "ARRAY",
               on: true,
               select: [t.s, fragment("?", a)]
           ) == [
             ["Hello", 1],
             ["Hello", 2],
             ["Hello", 3],
             ["World", 1],
             ["World", 2],
             ["World", 3],
             ["Goodbye", 1],
             ["Goodbye", 2],
             ["Goodbye", 3]
           ]

    TestRepo.query!(
      "CREATE TABLE nested_test(s String, nest Nested(x UInt8, y UInt32)) ENGINE = Memory"
    )

    on_exit(fn -> TestRepo.query!("DROP TABLE nested_test") end)

    TestRepo.query!(
      "INSERT INTO nested_test VALUES ('Hello', [1,2], [10,20]), ('World', [3,4,5], [30,40,50]), ('Goodbye', [], [])"
    )

    # SELECT s, `nest.x`, `nest.y` FROM nested_test ARRAY JOIN nest;
    assert TestRepo.all(
             from t in "nested_test",
               join: n in "nest",
               on: true,
               hints: "ARRAY",
               select: [t.s, n.x, n.y]
           ) ==
             [
               ["Hello", 1, 10],
               ["Hello", 2, 20],
               ["World", 3, 30],
               ["World", 4, 40],
               ["World", 5, 50]
             ]
  end

  # https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00049_any_left_join.sql
  # https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00049_any_left_join.reference
  test "00049_any_left_join" do
    # SELECT number, joined FROM system.numbers ANY LEFT JOIN (SELECT number * 2 AS number, number * 10 + 1 AS joined FROM system.numbers LIMIT 10) js2 USING number LIMIT 10
    # 0	1
    # 1	0
    # 2	21
    # 3	0
    # 4	41
    # 5	0
    # 6	61
    # 7	0
    # 8	81
    # 9	0

    sq =
      from n in fragment("numbers(10)"),
        select: %{
          number: selected_as(n.number * 2, :number),
          joined: selected_as(:number) * 10 + 1
        }

    assert TestRepo.all(
             from n1 in fragment("numbers(10)"),
               left_join: n2 in subquery(sq),
               on: n1.number == n2.number,
               hints: "ANY",
               select: [n1.number, n2.joined],
               order_by: fragment("ALL")
           ) == [
             [0, 1],
             [1, 0],
             [2, 21],
             [3, 0],
             [4, 41],
             [5, 0],
             [6, 61],
             [7, 0],
             [8, 81],
             [9, 0]
           ]
  end

  # https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00050_any_left_join.sql
  # https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00050_any_left_join.reference
  test "00050_any_left_join" do
    # SELECT a.*, b.* FROM
    # (
    #     SELECT number AS k FROM system.numbers LIMIT 10
    # ) AS a
    # ANY LEFT JOIN
    # (
    #     SELECT number * 2 AS k, number AS joined FROM system.numbers LIMIT 10
    # ) AS b
    # USING k
    # ORDER BY k;
    # 0	0	0
    # 1	0	0
    # 2	2	1
    # 3	0	0
    # 4	4	2
    # 5	0	0
    # 6	6	3
    # 7	0	0
    # 8	8	4
    # 9	0	0

    sq1 = from n in fragment("numbers(10)"), select: %{k: n.number}
    sq2 = from n in fragment("numbers(10)"), select: %{k: n.number * 2, joined: n.number}

    assert TestRepo.all(
             from a in subquery(sq1),
               left_join: b in subquery(sq2),
               on: a.k == b.k,
               hints: "ANY",
               order_by: a.k,
               select: [a.k, b.k, b.joined]
           ) == [
             [0, 0, 0],
             [1, 0, 0],
             [2, 2, 1],
             [3, 0, 0],
             [4, 4, 2],
             [5, 0, 0],
             [6, 6, 3],
             [7, 0, 0],
             [8, 8, 4],
             [9, 0, 0]
           ]
  end

  # https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00051_any_inner_join.sql
  # https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00051_any_inner_join.reference
  test "00051_any_inner_join" do
    # SELECT a.*, b.* FROM
    # (
    #     SELECT number AS k FROM system.numbers LIMIT 10
    # ) AS a
    # ANY INNER JOIN
    # (
    #     SELECT number * 2 AS k, number AS joined FROM system.numbers LIMIT 10
    # ) AS b
    # USING k;
    # 0	0	0
    # 2	2	1
    # 4	4	2
    # 6	6	3
    # 8	8	4

    sq1 = from n in fragment("numbers(10)"), select: %{k: n.number}
    sq2 = from n in fragment("numbers(10)"), select: %{k: n.number * 2, joined: n.number}

    assert TestRepo.all(
             from a in subquery(sq1),
               inner_join: b in subquery(sq2),
               on: a.k == b.k,
               hints: "ANY",
               select: [a.k, b.k, b.joined],
               order_by: fragment("ALL")
           ) == [
             [0, 0, 0],
             [2, 2, 1],
             [4, 4, 2],
             [6, 6, 3],
             [8, 8, 4]
           ]
  end

  # https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00976_asof_join_on.sql.j2
  # https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00976_asof_join_on.reference
  test "00976_asof_join_on" do
    TestRepo.query!("CREATE TABLE 00976_A(a UInt32, t UInt32) ENGINE = Memory")
    on_exit(fn -> TestRepo.query!("DROP TABLE 00976_A") end)

    TestRepo.query!("CREATE TABLE 00976_B(b UInt32, t UInt32) ENGINE = Memory")
    on_exit(fn -> TestRepo.query!("DROP TABLE 00976_B") end)

    TestRepo.query!(
      "INSERT INTO 00976_A (a,t) VALUES (1,1),(1,2),(1,3), (2,1),(2,2),(2,3), (3,1),(3,2),(3,3)"
    )

    TestRepo.query!("INSERT INTO 00976_B (b,t) VALUES (1,2),(1,4),(2,3)")

    # SELECT A.a, A.t, B.b, B.t FROM A ASOF LEFT JOIN B ON A.a == B.b AND A.t >= B.t ORDER BY (A.a, A.t);
    # 1	1	0	0
    # 1	2	1	2
    # 1	3	1	2
    # 2	1	0	0
    # 2	2	0	0
    # 2	3	2	3
    # 3	1	0	0
    # 3	2	0	0
    # 3	3	0	0
    assert TestRepo.all(
             from a in "00976_A",
               left_join: b in "00976_B",
               on: a.a == b.b and a.t >= b.t,
               hints: "ASOF",
               order_by: [a.a, a.t],
               select: [a.a, a.t, b.b, b.t]
           ) == [
             [1, 1, 0, 0],
             [1, 2, 1, 2],
             [1, 3, 1, 2],
             [2, 1, 0, 0],
             [2, 2, 0, 0],
             [2, 3, 2, 3],
             [3, 1, 0, 0],
             [3, 2, 0, 0],
             [3, 3, 0, 0]
           ]

    # SELECT count() FROM A ASOF LEFT JOIN B ON A.a == B.b AND B.t <= A.t;
    # 9
    assert TestRepo.aggregate(
             from(a in "00976_A",
               left_join: b in "00976_B",
               on: a.a == b.b and b.t <= a.t,
               hints: "ASOF"
             ),
             :count
           ) == 9

    # SELECT A.a, A.t, B.b, B.t FROM A ASOF INNER JOIN B ON B.t <= A.t AND A.a == B.b ORDER BY (A.a, A.t);
    # 1	2	1	2
    # 1	3	1	2
    # 2	3	2	3
    assert TestRepo.all(
             from a in "00976_A",
               inner_join: b in "00976_B",
               on: b.t <= a.t and a.a == b.b,
               hints: "ASOF",
               order_by: [a.a, a.t],
               select: [a.a, a.t, b.b, b.t]
           ) == [
             [1, 2, 1, 2],
             [1, 3, 1, 2],
             [2, 3, 2, 3]
           ]

    # SELECT A.a, A.t, B.b, B.t FROM A ASOF JOIN B ON A.a == B.b AND A.t <= B.t ORDER BY (A.a, A.t);
    # 1	1	1	2
    # 1	2	1	2
    # 1	3	1	4
    # 2	1	2	3
    # 2	2	2	3
    # 2	3	2	3
    assert TestRepo.all(
             from a in "00976_A",
               join: b in "00976_B",
               on: a.a == b.b and a.t <= b.t,
               hints: "ASOF",
               order_by: [a.a, a.t],
               select: [a.a, a.t, b.b, b.t]
           ) == [
             [1, 1, 1, 2],
             [1, 2, 1, 2],
             [1, 3, 1, 4],
             [2, 1, 2, 3],
             [2, 2, 2, 3],
             [2, 3, 2, 3]
           ]

    # SELECT A.a, A.t, B.b, B.t FROM A ASOF JOIN B ON A.a == B.b AND B.t >= A.t ORDER BY (A.a, A.t);
    # 1	1	1	2
    # 1	2	1	2
    # 1	3	1	4
    # 2	1	2	3
    # 2	2	2	3
    # 2	3	2	3
    assert TestRepo.all(
             from a in "00976_A",
               join: b in "00976_B",
               on: a.a == b.b and b.t >= a.t,
               hints: "ASOF",
               order_by: [a.a, a.t],
               select: [a.a, a.t, b.b, b.t]
           ) == [
             [1, 1, 1, 2],
             [1, 2, 1, 2],
             [1, 3, 1, 4],
             [2, 1, 2, 3],
             [2, 2, 2, 3],
             [2, 3, 2, 3]
           ]

    # SELECT A.a, A.t, B.b, B.t FROM A ASOF JOIN B ON A.a == B.b AND A.t > B.t ORDER BY (A.a, A.t);
    # 1	3	1	2
    assert TestRepo.all(
             from a in "00976_A",
               join: b in "00976_B",
               on: a.a == b.b and a.t > b.t,
               hints: "ASOF",
               order_by: [a.a, a.t],
               select: [a.a, a.t, b.b, b.t]
           ) == [
             [1, 3, 1, 2]
           ]

    # SELECT A.a, A.t, B.b, B.t FROM A ASOF JOIN B ON A.a == B.b AND A.t < B.t ORDER BY (A.a, A.t);
    # 1	1	1	2
    # 1	2	1	4
    # 1	3	1	4
    # 2	1	2	3
    # 2	2	2	3
    assert TestRepo.all(
             from a in "00976_A",
               join: b in "00976_B",
               on: a.a == b.b and a.t < b.t,
               hints: "ASOF",
               order_by: [a.a, a.t],
               select: [a.a, a.t, b.b, b.t]
           ) == [
             [1, 1, 1, 2],
             [1, 2, 1, 4],
             [1, 3, 1, 4],
             [2, 1, 2, 3],
             [2, 2, 2, 3]
           ]

    # SELECT count() FROM A ASOF JOIN B ON A.a == B.b AND A.t == B.t; -- { serverError 403 }
    assert_raise Ch.Error, ~r/INVALID_JOIN_ON_EXPRESSION/, fn ->
      TestRepo.all(
        from a in "00976_A",
          join: b in "00976_B",
          on: a.a == b.b and a.t == b.t,
          hints: "ASOF",
          select: count()
      )
    end

    # SELECT count() FROM A ASOF JOIN B ON A.a == B.b AND A.t != B.t; -- { serverError 403 }
    assert_raise Ch.Error, ~r/INVALID_JOIN_ON_EXPRESSION/, fn ->
      TestRepo.all(
        from a in "00976_A",
          join: b in "00976_B",
          on: a.a == b.b and a.t != b.t,
          hints: "ASOF",
          select: count()
      )
    end

    expected_error =
      if clickhouse_version() >= [25, 2] do
        ~r/INVALID_JOIN_ON_EXPRESSION/
      else
        ~r/NOT_IMPLEMENTED/
      end

    # SELECT A.a, A.t, B.b, B.t FROM A ASOF JOIN B ON A.a == B.b AND A.t < B.t OR A.a == B.b + 1 ORDER BY (A.a, A.t); -- { serverError 48 }
    assert_raise Ch.Error, expected_error, fn ->
      TestRepo.all(
        from a in "00976_A",
          join: b in "00976_B",
          on: (a.a == b.b and a.t < b.t) or a.a == b.b + 1,
          hints: "ASOF",
          order_by: [a.a, a.t],
          select: [a.a, a.t, b.b, b.t]
      )
    end

    # TODO?
    # SELECT A.a, A.t, B.b, B.t FROM A
    # ASOF INNER JOIN (SELECT * FROM B UNION ALL SELECT 1, 3) AS B ON B.t <= A.t AND A.a == B.b
    # WHERE B.t != 3 ORDER BY (A.a, A.t);
    # 1	2	1	2
  end

  # https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00927_asof_joins.sql
  # https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00927_asof_joins.reference
  test "00927_asof_joins" do
    TestRepo.query!(
      "CREATE TABLE md(key UInt32, t DateTime, bid Float64, ask Float64) ENGINE = MergeTree() ORDER BY (key, t)"
    )

    on_exit(fn -> TestRepo.query!("DROP TABLE md") end)

    TestRepo.query!(
      "INSERT INTO md(key,t,bid,ask) VALUES (1,20,7,8),(1,5,1,2),(1,10,11,12),(1,15,5,6)"
    )

    TestRepo.query!(
      "INSERT INTO md(key,t,bid,ask) VALUES (2,20,17,18),(2,5,11,12),(2,10,21,22),(2,15,5,6)"
    )

    TestRepo.query!(
      "CREATE TABLE tv(key UInt32, t DateTime, tv Float64) ENGINE = MergeTree() ORDER BY (key, t)"
    )

    on_exit(fn -> TestRepo.query!("DROP TABLE tv") end)

    TestRepo.query!(
      "INSERT INTO tv(key,t,tv) VALUES (1,5,1.5),(1,6,1.51),(1,10,11.5),(1,11,11.51),(1,15,5.5),(1,16,5.6),(1,20,7.5)"
    )

    TestRepo.query!(
      "INSERT INTO tv(key,t,tv) VALUES (2,5,2.5),(2,6,2.51),(2,10,12.5),(2,11,12.51),(2,15,6.5),(2,16,5.6),(2,20,8.5)"
    )

    # SELECT tv.key, toString(tv.t, 'UTC'), md.bid, tv.tv, md.ask FROM tv ASOF LEFT JOIN md USING(key,t) ORDER BY (tv.key, tv.t);
    # 1	1970-01-01 00:00:05	1	1.5	2
    # 1	1970-01-01 00:00:06	1	1.51	2
    # 1	1970-01-01 00:00:10	11	11.5	12
    # 1	1970-01-01 00:00:11	11	11.51	12
    # 1	1970-01-01 00:00:15	5	5.5	6
    # 1	1970-01-01 00:00:16	5	5.6	6
    # 1	1970-01-01 00:00:20	7	7.5	8
    # 2	1970-01-01 00:00:05	11	2.5	12
    # 2	1970-01-01 00:00:06	11	2.51	12
    # 2	1970-01-01 00:00:10	21	12.5	22
    # 2	1970-01-01 00:00:11	21	12.51	22
    # 2	1970-01-01 00:00:15	5	6.5	6
    # 2	1970-01-01 00:00:16	5	5.6	6
    # 2	1970-01-01 00:00:20	17	8.5	18
    assert TestRepo.all(
             from tv in "tv",
               left_join: md in "md",
               on: tv.key == md.key and tv.t >= md.t,
               hints: "ASOF",
               order_by: [tv.key, tv.t],
               select: [tv.key, fragment("toString(?,'UTC')", tv.t), md.bid, tv.tv, md.ask]
           ) == [
             [1, "1970-01-01 00:00:05", 1.0, 1.5, 2.0],
             [1, "1970-01-01 00:00:06", 1.0, 1.51, 2.0],
             [1, "1970-01-01 00:00:10", 11.0, 11.5, 12.0],
             [1, "1970-01-01 00:00:11", 11.0, 11.51, 12.0],
             [1, "1970-01-01 00:00:15", 5.0, 5.5, 6.0],
             [1, "1970-01-01 00:00:16", 5.0, 5.6, 6.0],
             [1, "1970-01-01 00:00:20", 7.0, 7.5, 8.0],
             [2, "1970-01-01 00:00:05", 11.0, 2.5, 12.0],
             [2, "1970-01-01 00:00:06", 11.0, 2.51, 12.0],
             [2, "1970-01-01 00:00:10", 21.0, 12.5, 22.0],
             [2, "1970-01-01 00:00:11", 21.0, 12.51, 22.0],
             [2, "1970-01-01 00:00:15", 5.0, 6.5, 6.0],
             [2, "1970-01-01 00:00:16", 5.0, 5.6, 6.0],
             [2, "1970-01-01 00:00:20", 17.0, 8.5, 18.0]
           ]
  end

  # https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/01031_new_any_join.sql.j2
  # https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/01031_new_any_join.reference.j2
  test "01031_new_any_join" do
    TestRepo.query!("CREATE TABLE any_t1 (x UInt32, s String) engine = Memory")
    on_exit(fn -> TestRepo.query!("DROP TABLE any_t1") end)
    TestRepo.query!("CREATE TABLE any_t2 (x UInt32, s String) engine = Memory")
    on_exit(fn -> TestRepo.query!("DROP TABLE any_t2") end)

    TestRepo.query!(
      "INSERT INTO any_t1 (x, s) VALUES (0, 'a1'), (1, 'a2'), (2, 'a3'), (3, 'a4'), (4, 'a5')"
    )

    TestRepo.query!("INSERT INTO any_t2 (x, s) VALUES (2, 'b1'), (4, 'b3'), (5, 'b6')")

    # SELECT t1.*, t2.* FROM t1 ANY LEFT JOIN t2 USING(x) ORDER BY t1.x, t2.x;
    # 0	a1	0
    # 1	a2	0
    # 2	a3	2	b1
    # 3	a4	0
    # 4	a5	4	b3
    assert TestRepo.all(
             from t1 in "any_t1",
               left_join: t2 in "any_t2",
               on: t1.x == t2.x,
               hints: "ANY",
               order_by: [t1.x, t2.x],
               select: [t1.x, t1.s, t2.x, t2.s]
           ) == [
             [0, "a1", 0, ""],
             [1, "a2", 0, ""],
             [2, "a3", 2, "b1"],
             [3, "a4", 0, ""],
             [4, "a5", 4, "b3"]
           ]

    # SELECT t1.*, t2.* FROM t2 ANY LEFT JOIN t1 USING(x) ORDER BY t1.x, t2.x;
    # 0		5	b6
    # 2	a3	2	b1
    # 4	a5	4	b3
    assert TestRepo.all(
             from t2 in "any_t2",
               left_join: t1 in "any_t1",
               on: t2.x == t1.x,
               hints: "ANY",
               order_by: [t1.x, t2.x],
               select: [t1.x, t1.s, t2.x, t2.s]
           ) == [
             [0, "", 5, "b6"],
             [2, "a3", 2, "b1"],
             [4, "a5", 4, "b3"]
           ]

    # SELECT t1.*, t2.* FROM t1 ANY INNER JOIN t2 USING(x) ORDER BY t1.x, t2.x;
    # 2	a3	2	b1
    # 4	a5	4	b3
    assert TestRepo.all(
             from t1 in "any_t1",
               inner_join: t2 in "any_t2",
               on: t1.x == t2.x,
               hints: "ANY",
               order_by: [t1.x, t2.x],
               select: [t1.x, t1.s, t2.x, t2.s]
           ) == [
             [2, "a3", 2, "b1"],
             [4, "a5", 4, "b3"]
           ]

    # SELECT t1.*, t2.* FROM t2 ANY INNER JOIN t1 USING(x) ORDER BY t1.x, t2.x;
    # 2	a3	2	b1
    # 4	a5	4	b3
    assert TestRepo.all(
             from t2 in "any_t2",
               inner_join: t1 in "any_t1",
               on: t2.x == t1.x,
               hints: "ANY",
               order_by: [t1.x, t2.x],
               select: [t1.x, t1.s, t2.x, t2.s]
           ) == [
             [2, "a3", 2, "b1"],
             [4, "a5", 4, "b3"]
           ]

    # SELECT t1.*, t2.* FROM t1 ANY RIGHT JOIN t2 USING(x) ORDER BY t1.x, t2.x;
    # 0		5	b6
    # 2	a3	2	b1
    # 4	a5	4	b3
    assert TestRepo.all(
             from t1 in "any_t1",
               right_join: t2 in "any_t2",
               on: t1.x == t2.x,
               hints: "ANY",
               order_by: [t1.x, t2.x],
               select: [t1.x, t1.s, t2.x, t2.s]
           ) == [
             [0, "", 5, "b6"],
             [2, "a3", 2, "b1"],
             [4, "a5", 4, "b3"]
           ]

    # SELECT t1.*, t2.* FROM t2 ANY RIGHT JOIN t1 USING(x) ORDER BY t1.x, t2.x;
    # 0	a1	0
    # 1	a2	0
    # 2	a3	2	b1
    # 3	a4	0
    # 4	a5	4	b3
    assert TestRepo.all(
             from t2 in "any_t2",
               right_join: t1 in "any_t1",
               on: t2.x == t1.x,
               hints: "ANY",
               order_by: [t1.x, t2.x],
               select: [t1.x, t1.s, t2.x, t2.s]
           ) == [
             [0, "a1", 0, ""],
             [1, "a2", 0, ""],
             [2, "a3", 2, "b1"],
             [3, "a4", 0, ""],
             [4, "a5", 4, "b3"]
           ]
  end

  # https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/01031_semi_anti_join.sql
  # https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/01031_semi_anti_join.reference
  test "01031_semi_anti_join" do
    TestRepo.query!("CREATE TABLE semi_anti_t1(x UInt32, s String) engine = Memory")
    on_exit(fn -> TestRepo.query!("DROP TABLE semi_anti_t1") end)
    TestRepo.query!("CREATE TABLE semi_anti_t2(x UInt32, s String) engine = Memory")
    on_exit(fn -> TestRepo.query!("DROP TABLE semi_anti_t2") end)

    TestRepo.query!(
      "INSERT INTO semi_anti_t1 (x, s) VALUES (0, 'a1'), (1, 'a2'), (2, 'a3'), (3, 'a4'), (4, 'a5'), (2, 'a6')"
    )

    TestRepo.query!(
      "INSERT INTO semi_anti_t2 (x, s) VALUES (2, 'b1'), (2, 'b2'), (4, 'b3'), (4, 'b4'), (4, 'b5'), (5, 'b6')"
    )

    # SELECT t1.*, t2.* FROM t1 SEMI LEFT JOIN t2 USING(x) ORDER BY t1.x, t2.x, t1.s, t2.s;
    # 2	a3	2	b1
    # 2	a6	2	b1
    # 4	a5	4	b3

    assert TestRepo.all(
             from t1 in "semi_anti_t1",
               left_join: t2 in "semi_anti_t2",
               on: t1.x == t2.x,
               hints: "SEMI",
               order_by: [t1.x, t2.x, t1.s, t2.s],
               select: [t1.x, t1.s, t2.x, t2.s]
           ) == [
             [2, "a3", 2, "b1"],
             [2, "a6", 2, "b1"],
             [4, "a5", 4, "b3"]
           ]

    # SELECT t1.*, t2.* FROM t1 SEMI RIGHT JOIN t2 USING(x) ORDER BY t1.x, t2.x, t1.s, t2.s;
    # 2	a3	2	b1
    # 2	a3	2	b2
    # 4	a5	4	b3
    # 4	a5	4	b4
    # 4	a5	4	b5

    assert TestRepo.all(
             from t1 in "semi_anti_t1",
               right_join: t2 in "semi_anti_t2",
               on: t1.x == t2.x,
               hints: "SEMI",
               order_by: [t1.x, t2.x, t1.s, t2.s],
               select: [t1.x, t1.s, t2.x, t2.s]
           ) == [
             [2, "a3", 2, "b1"],
             [2, "a3", 2, "b2"],
             [4, "a5", 4, "b3"],
             [4, "a5", 4, "b4"],
             [4, "a5", 4, "b5"]
           ]

    # SELECT t1.*, t2.* FROM t1 ANTI LEFT JOIN t2 USING(x) ORDER BY t1.x, t2.x, t1.s, t2.s;
    # 0	a1	0
    # 1	a2	1
    # 3	a4	3

    assert TestRepo.all(
             from t1 in "semi_anti_t1",
               left_join: t2 in "semi_anti_t2",
               on: t1.x == t2.x,
               hints: "ANTI",
               order_by: [t1.x, t2.x, t1.s, t2.s],
               select: [t1.x, t1.s, t2.x, t2.s]
           ) == [
             [0, "a1", 0, ""],
             [1, "a2", 1, ""],
             [3, "a4", 3, ""]
           ]

    # SELECT t1.*, t2.* FROM t1 ANTI RIGHT JOIN t2 USING(x) ORDER BY t1.x, t2.x, t1.s, t2.s;
    # 0		5	b6

    assert TestRepo.all(
             from t1 in "semi_anti_t1",
               right_join: t2 in "semi_anti_t2",
               on: t1.x == t2.x,
               hints: "ANTI",
               order_by: [t1.x, t2.x, t1.s, t2.s],
               select: [t1.x, t1.s, t2.x, t2.s]
           ) == [
             [0, "", 5, "b6"]
           ]
  end

  # https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/01332_join_type_syntax_position.sql
  # https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/01332_join_type_syntax_position.reference
  test "01332_join_type_syntax_position" do
    # select * from numbers(1) t1 left outer join numbers(1) t2 using number;
    assert TestRepo.all(
             from t1 in fragment("numbers(1)"),
               left_join: t2 in fragment("numbers(1)"),
               on: t1.number == t2.number,
               select: [t1.number, t2.number]
           ) == [[0, 0]]

    # select * from numbers(1) t1 right outer join numbers(1) t2 using number;
    assert TestRepo.all(
             from t1 in fragment("numbers(1)"),
               right_join: t2 in fragment("numbers(1)"),
               on: t1.number == t2.number,
               select: [t1.number, t2.number]
           ) == [[0, 0]]

    # select * from numbers(1) t1 left any join numbers(1) t2 using number;
    assert TestRepo.all(
             from t1 in fragment("numbers(1)"),
               left_join: t2 in fragment("numbers(1)"),
               on: t1.number == t2.number,
               hints: "ANY",
               select: [t1.number, t2.number]
           ) == [[0, 0]]

    # select * from numbers(1) t1 right any join numbers(1) t2 using number;
    assert TestRepo.all(
             from t1 in fragment("numbers(1)"),
               right_join: t2 in fragment("numbers(1)"),
               on: t1.number == t2.number,
               hints: "ANY",
               select: [t1.number, t2.number]
           ) == [[0, 0]]

    # select * from numbers(1) t1 left semi join numbers(1) t2 using number;
    assert TestRepo.all(
             from t1 in fragment("numbers(1)"),
               left_join: t2 in fragment("numbers(1)"),
               on: t1.number == t2.number,
               hints: "SEMI",
               select: [t1.number, t2.number]
           ) == [[0, 0]]

    # select * from numbers(1) t1 right semi join numbers(1) t2 using number;
    assert TestRepo.all(
             from t1 in fragment("numbers(1)"),
               right_join: t2 in fragment("numbers(1)"),
               on: t1.number == t2.number,
               hints: "SEMI",
               select: [t1.number, t2.number]
           ) == [[0, 0]]

    # select * from numbers(1) t1 left anti join numbers(1) t2 using number;
    assert TestRepo.all(
             from t1 in fragment("numbers(1)"),
               left_join: t2 in fragment("numbers(1)"),
               on: t1.number == t2.number,
               hints: "ANTI",
               select: [t1.number, t2.number]
           ) == []

    # select * from numbers(1) t1 right anti join numbers(1) t2 using number;
    assert TestRepo.all(
             from t1 in fragment("numbers(1)"),
               right_join: t2 in fragment("numbers(1)"),
               on: t1.number == t2.number,
               hints: "ANTI",
               select: [t1.number, t2.number]
           ) == []

    # select * from numbers(1) t1 asof join numbers(1) t2 using number; -- { serverError 62 }
    assert_raise Ch.Error, ~r/INVALID_JOIN_ON_EXPRESSION/, fn ->
      TestRepo.all(
        from t1 in fragment("numbers(1)"),
          join: t2 in fragment("numbers(1)"),
          on: t1.number == t2.number,
          hints: "ASOF",
          select: [t1.number, t2.number]
      )
    end

    # select * from numbers(1) t1 left asof join numbers(1) t2 using number; -- { serverError 62 }
    assert_raise Ch.Error, ~r/INVALID_JOIN_ON_EXPRESSION/, fn ->
      TestRepo.all(
        from t1 in fragment("numbers(1)"),
          left_join: t2 in fragment("numbers(1)"),
          on: t1.number == t2.number,
          hints: "ASOF",
          select: [t1.number, t2.number]
      )
    end
  end
end
