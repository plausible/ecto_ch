defmodule Ecto.Integration.SQLTest do
  use Ecto.Integration.Case

  alias Ecto.Integration.{TestRepo, Barebone}
  alias Ecto.Integration.{Post, Tag}

  import Ecto.Query

  test "fragmented types" do
    datetime = ~N[2014-01-16 20:26:51]

    TestRepo.insert!(%Post{inserted_at: datetime})

    query =
      from p in Post, where: fragment("? >= ?", p.inserted_at, ^datetime), select: p.inserted_at

    assert [^datetime] = TestRepo.all(query)
  end

  test "fragmented schemaless types" do
    TestRepo.insert!(%Post{visits: 123})
    assert [123] = TestRepo.all(from p in "posts", select: type(fragment("visits"), :integer))
  end

  test "fragment array types" do
    text1 = "foo"
    text2 = "bar"
    result = TestRepo.query!("SELECT {$0:Array(String)}", [[text1, text2]])
    assert result.rows == [[[text1, text2]]]
  end

  test "Converts empty array correctly" do
    result = TestRepo.query!("SELECT [1,2,3] = {$0:Array(UInt8)}", [[]])
    assert result.rows == [[0]]

    result = TestRepo.query!("SELECT [] = {$0:Array(UInt8)}", [[]])
    assert result.rows == [[1]]

    %{id: tag_id} = TestRepo.insert!(%Tag{id: 1, uuids: []})
    query = from t in Tag, where: t.uuids == []
    assert [%{id: ^tag_id}] = TestRepo.all(query)
  end

  test "query!/4 with dynamic repo" do
    TestRepo.put_dynamic_repo(:unknown)
    assert_raise RuntimeError, ~r/:unknown/, fn -> TestRepo.query!("SELECT 1") end
  end

  test "query!/4" do
    result = TestRepo.query!("SELECT 1")
    assert result.rows == [[1]]
  end

  test "query!/4 with iodata" do
    result = TestRepo.query!(["SELECT", ?\s, ?1])
    assert result.rows == [[1]]
  end

  test "disconnect_all/2" do
    # TODO PoolRepo?
    assert :ok = TestRepo.disconnect_all(0)
  end

  test "to_sql/3" do
    {sql, []} = TestRepo.to_sql(:all, Barebone)
    assert sql == ~s[SELECT b0."num" FROM "barebones" AS b0]

    # {sql, [0]} = TestRepo.to_sql(:update_all, from(b in Barebone, update: [set: [num: ^0]]))
    # assert sql =~ "UPDATE"
    # assert sql =~ "barebones"
    # assert sql =~ "SET"

    {sql, []} = TestRepo.to_sql(:delete_all, Barebone)
    assert sql == ~s[DELETE FROM "barebones" WHERE 1]
  end

  @tag skip: true
  test "raises when primary key is not unique on struct operation"

  test "Repo.insert! escape" do
    TestRepo.insert!(%Post{title: "'"})

    query = from(p in Post, select: p.title)
    assert ["'"] == TestRepo.all(query)
  end

  @tag skip: true
  test "Repo.update! escape"

  test "Repo.insert_all escape" do
    TestRepo.insert_all(Post, [%{title: "'"}])

    query = from(p in Post, select: p.title)
    assert ["'"] == TestRepo.all(query)
  end

  @tag skip: true
  test "Repo.update_all escape"

  test "Repo.delete_all escape" do
    TestRepo.insert!(%Post{title: "hello"})
    assert [_] = TestRepo.all(Post)

    TestRepo.delete_all(from(Post, where: "'" == "'"),
      settings: [allow_experimental_lightweight_delete: 1, mutations_sync: 1]
    )

    assert [] == TestRepo.all(Post)
  end

  @tag skip: true
  test "load" do
    inserted_at = ~N[2016-01-01 09:00:00]
    TestRepo.insert!(%Post{title: "title1", inserted_at: inserted_at, public: false})

    result = Ecto.Adapters.SQL.query!(TestRepo, "SELECT * FROM posts", [])
    posts = Enum.map(result.rows, &TestRepo.load(Post, {result.columns, &1}))
    assert [%Post{title: "title1", inserted_at: ^inserted_at, public: false}] = posts
  end

  test "returns true when table exists" do
    assert Ecto.Adapters.SQL.table_exists?(TestRepo, "posts")
  end

  test "returns false table doesn't exists" do
    refute Ecto.Adapters.SQL.table_exists?(TestRepo, "unknown")
  end

  @tag skip: true
  test "returns result as a formatted table"

  @tag skip: true
  test "format_table edge cases"
end
