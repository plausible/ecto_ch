defmodule Ecto.Integration.SQLTest do
  use Ecto.Integration.Case

  alias Ecto.Adapters.ClickHouse.Connection
  alias Ecto.Integration.{TestRepo, Barebone}
  alias Ecto.Integration.{Post, Tag}

  import Ecto.Query

  defmodule QuotedNames do
    use Ecto.Schema

    @primary_key false
    @table_name "ecto_ch quoted ' \" ` \\ ; -- /* */ $tag$ table \\"
    @column_name String.to_atom("value ' \" ` \\ ; -- /* */ $tag$ column \\")

    schema @table_name do
      field :value, :string, source: @column_name
    end

    def table_name, do: @table_name
    def column_name, do: Atom.to_string(@column_name)
  end

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

  test "quoted strings and identifiers cannot break out into ClickHouse syntax" do
    string = ~S|value' FROM numbers(10) -- \ $tag$body$tag$ /* comment */|
    result = TestRepo.query!(["SELECT ", Connection.quote_string(string)])

    assert result.rows == [[string]]

    for {quoter, name} <- [
          {?\", ~S|alias" FROM numbers(10) -- \ $tag$body$tag$ /* comment */|},
          {?`, ~S|alias` FROM numbers(10) -- \ $tag$body$tag$ /* comment */|}
        ] do
      result = TestRepo.query!(["SELECT 1 AS ", Connection.quote_name(name, quoter)])

      assert result.columns == [name]
      assert result.rows == [[1]]
    end
  end

  test "bound parameters and inline literals round-trip escaping edge cases" do
    values = [
      "",
      "'",
      "\\",
      ~S|\'|,
      "'\\",
      "\\\\'",
      "single ' double \" backtick ` backslash \\ middle",
      "ends with \\",
      "; SELECT 2; -- /* comment */ $tag$ heredoc $tag$",
      "line one\nline two\r\n\t",
      "\u00E9\u0416\u4E2D\u6587",
      :binary.list_to_bin(Enum.to_list(0..127))
    ]

    for value <- values do
      query =
        from _ in "one",
          select: {type(^value, :string), fragment("?", constant(^value))}

      assert TestRepo.one(query, database: "system") == {value, value}
    end
  end

  test "quoted table, column, and alias names round-trip through ClickHouse" do
    table = QuotedNames.table_name()
    column = QuotedNames.column_name()
    quoted_table = Connection.quote_name(table)
    quoted_column = Connection.quote_name(column)

    TestRepo.query!([
      "CREATE TABLE ",
      quoted_table,
      " (",
      quoted_column,
      " String) ENGINE Memory"
    ])

    on_exit(fn -> TestRepo.query!(["DROP TABLE IF EXISTS ", quoted_table]) end)

    value = "value with ' \" ` \\ and \\' adjacent"
    assert %QuotedNames{value: ^value} = TestRepo.insert!(%QuotedNames{value: value})
    assert [^value] = TestRepo.all(from row in QuotedNames, select: row.value)

    alias_name = "alias ' \" ` \\ ; -- /* */ $tag$ \\"

    result =
      TestRepo.query!([
        "SELECT ",
        quoted_column,
        " AS ",
        Connection.quote_name(alias_name, ?`),
        " FROM ",
        quoted_table
      ])

    assert result.columns == [alias_name]
    assert result.rows == [[value]]
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
