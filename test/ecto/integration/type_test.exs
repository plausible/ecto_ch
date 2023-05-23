defmodule Ecto.Integration.TypeTest do
  use Ecto.Integration.Case
  import Ecto.Query

  alias Ecto.Integration.TestRepo
  alias Ecto.Integration.{Comment, Post, Tag}

  @parameterized_type Ecto.ParameterizedType.init(Ecto.Enum, values: [:a, :b])

  test "primitive types" do
    integer = 1
    float = 0.1
    blob = <<0, 1>>
    uuid = "00010203-0405-4607-8809-0a0b0c0d0e0f"
    datetime = ~N[2014-01-16 20:26:51]

    TestRepo.insert!(%Post{
      blob: blob,
      public: true,
      visits: integer,
      uuid: uuid,
      counter: integer,
      inserted_at: datetime,
      intensity: float
    })

    # nil
    assert [nil] = TestRepo.all(from Post, select: nil)

    # ID
    assert [1] =
             TestRepo.all(
               from p in Post,
                 where: p.counter == ^integer,
                 select: p.counter
             )

    # Integers
    assert [1] = TestRepo.all(from p in Post, where: p.visits == ^integer, select: p.visits)
    assert [1] = TestRepo.all(from p in Post, where: p.visits == 1, select: p.visits)
    assert [3] = TestRepo.all(from p in Post, select: p.visits + 2)

    # TODO
    # Floats
    # assert [0.1] = TestRepo.all(from p in Post, where: p.intensity == ^float, select: p.intensity)
    # assert [0.1] = TestRepo.all(from p in Post, where: p.intensity == 0.1, select: p.intensity)
    assert [1500.0] = TestRepo.all(from p in Post, select: 1500.0)
    assert [intensity_x5] = TestRepo.all(from p in Post, select: p.intensity * 5)
    assert_in_delta intensity_x5, 0.5, 0.000001

    # Booleans
    assert [true] =
             TestRepo.all(
               from p in Post,
                 where: p.public == ^true,
                 select: p.public
             )

    assert [true] =
             TestRepo.all(
               from p in Post,
                 where: p.public == true,
                 select: p.public
             )

    # Binaries
    assert [^blob] =
             TestRepo.all(
               from p in Post,
                 where: p.blob == <<0, 1>>,
                 select: p.blob
             )

    assert [^blob] =
             TestRepo.all(
               from p in Post,
                 where: p.blob == ^blob,
                 select: p.blob
             )

    # UUID
    assert [^uuid] =
             TestRepo.all(
               from p in Post,
                 where: p.uuid == ^uuid,
                 select: p.uuid
             )

    # NaiveDatetime
    assert [^datetime] =
             TestRepo.all(
               from p in Post,
                 where: p.inserted_at == ^datetime,
                 select: p.inserted_at
             )

    # TODO
    # Datetime
    # datetime = DateTime.from_unix!(System.os_time(:second), :second)
    # TestRepo.insert!(%User{inserted_at: datetime})

    # assert [^datetime] =
    #          TestRepo.all(
    #            from u in User,
    #              where: u.inserted_at == ^datetime,
    #              select: u.inserted_at
    #          )

    # usec
    # naive_datetime = ~N[2014-01-16 20:26:51.000000]
    # datetime = DateTime.from_naive!(~N[2014-01-16 20:26:51.000000], "Etc/UTC")
    # TestRepo.insert!(%Usec{naive_datetime_usec: naive_datetime, utc_datetime_usec: datetime})
    # assert [^naive_datetime] = TestRepo.all(from u in Usec, where: u.naive_datetime_usec == ^naive_datetime, select: u.naive_datetime_usec)
    # assert [^datetime] = TestRepo.all(from u in Usec, where: u.utc_datetime_usec == ^datetime, select: u.utc_datetime_usec)

    # naive_datetime = ~N[2014-01-16 20:26:51.123000]
    # datetime = DateTime.from_naive!(~N[2014-01-16 20:26:51.123000], "Etc/UTC")
    # TestRepo.insert!(%Usec{naive_datetime_usec: naive_datetime, utc_datetime_usec: datetime})
    # assert [^naive_datetime] = TestRepo.all(from u in Usec, where: u.naive_datetime_usec == ^naive_datetime, select: u.naive_datetime_usec)
    # assert [^datetime] = TestRepo.all(from u in Usec, where: u.utc_datetime_usec == ^datetime, select: u.utc_datetime_usec)
  end

  test "utf8 strings" do
    # :string loader ensures behaviour similar to
    # https://clickhouse.com/docs/en/sql-reference/functions/string-functions/#tovalidutf8
    TestRepo.insert!(%Post{title: "\x61\xF0\x80\x80\x80b"})
    assert %Post{title: "a�b"} = TestRepo.one!(Post)
    assert ["a�b"] = TestRepo.all(from p in Post, select: p.title)
    assert ["a�b"] = TestRepo.all(from p in "posts", select: p.title)
  end

  # TODO find a way to not process :binary as utf8
  @tag skip: true
  test "non utf8 binary" do
    value = "\x61\xF0\x80\x80\x80b"
    TestRepo.insert!(%Post{blob: "\x61\xF0\x80\x80\x80b"})
    assert %Post{blob: ^value} = TestRepo.one!(Post)
    assert [^value] = TestRepo.all(from p in Post, select: p.blob)
    assert [^value] = TestRepo.all(from p in "posts", select: p.blob)
  end

  test "primitive types boolean negate" do
    TestRepo.insert!(%Post{public: true})

    assert [false] =
             TestRepo.all(
               from p in Post,
                 where: p.public == true,
                 select: not p.public
             )

    assert [true] =
             TestRepo.all(
               from p in Post,
                 where: p.public == true,
                 select: not not p.public
             )
  end

  test "aggregate types" do
    datetime = ~N[2014-01-16 20:26:51]
    TestRepo.insert!(%Post{inserted_at: datetime})
    query = from p in Post, select: max(p.inserted_at)
    assert [^datetime] = TestRepo.all(query)
  end

  @tag skip: true
  test "aggregate custom types"

  test "aggregate filter types" do
    datetime = ~N[2014-01-16 20:26:51]
    TestRepo.insert!(%Post{inserted_at: datetime})

    query =
      from p in Post,
        select: filter(max(p.inserted_at), p.public == ^true)

    assert [^datetime] = TestRepo.all(query)
  end

  test "coalesce text type when default" do
    TestRepo.insert!(%Post{blob: nil})
    blob = <<0, 1>>

    query = from p in Post, select: coalesce(p.blob, ^blob)
    assert [""] = TestRepo.all(query)

    query = from p in Post, select: coalesce(fragment("nullIf(?, '')", p.blob), ^blob)
    assert [^blob] = TestRepo.all(query)
  end

  test "coalesce text type when value" do
    blob = <<0, 2>>
    default_blob = <<0, 1>>
    TestRepo.insert!(%Post{blob: blob})
    query = from p in Post, select: coalesce(p.blob, ^default_blob)
    assert [^blob] = TestRepo.all(query)
  end

  @float64 Ecto.ParameterizedType.init(Ch, type: "Float64")
  test "tagged types" do
    %{id: post_id} = TestRepo.insert!(%Post{id: 1, visits: 12})
    TestRepo.insert!(%Comment{text: "#{post_id}", post_id: post_id})

    # Numbers
    assert [1] = TestRepo.all(from Post, select: type(^"1", :integer))
    assert [1.0] = TestRepo.all(from Post, select: type(^1.0, ^@float64))
    assert [1] = TestRepo.all(from p in Post, select: type(^"1", p.visits))
    assert [1.0] = TestRepo.all(from p in Post, select: type(^"1", p.intensity))

    # TODO
    # Custom wrappers
    # assert [1] = TestRepo.all(from Post, select: type(^"1", CustomPermalink))

    # Custom types
    uuid = Ecto.UUID.generate()
    assert [^uuid] = TestRepo.all(from Post, select: type(^uuid, Ecto.UUID))

    # Parameterized types
    assert [:a] = TestRepo.all(from Post, select: type(^"a", ^@parameterized_type))

    # Math operations
    assert [4] = TestRepo.all(from Post, select: type(2 + ^"2", :integer))
    # assert [4.0] = TestRepo.all(from Post, select: type(2.0 + ^"2", ^@float64))
    assert [4] = TestRepo.all(from p in Post, select: type(2 + ^"2", p.visits))
    assert [4.0] = TestRepo.all(from p in Post, select: type(2.0 + ^"2", p.intensity))

    # Comparison expression
    assert [12] = TestRepo.all(from p in Post, select: type(coalesce(p.visits, 0), :integer))

    assert [0.0] =
             TestRepo.all(from p in Post, select: type(coalesce(p.intensity, 1.0), ^@float64))

    assert [1.0] =
             TestRepo.all(
               from p in Post,
                 select: type(coalesce(fragment("nullIf(?, 0)", p.intensity), 1.0), ^@float64)
             )

    # parent_as/1
    child =
      from c in Comment,
        where: type(parent_as(:posts).id, :string) == c.text,
        select: c.post_id

    query =
      from p in Post,
        as: :posts,
        where: p.id in subquery(child),
        select: p.id

    assert_raise Ch.Error, ~r/UNKNOWN_IDENTIFIER/, fn ->
      TestRepo.all(query)
    end
  end

  @tag skip: true
  test "binary id type"

  test "text type as blob" do
    assert %Post{} = post = TestRepo.insert!(%Post{id: 1, blob: <<0, 1, 2>>})
    id = post.id
    assert post.blob == <<0, 1, 2>>
    assert [^id] = TestRepo.all(from p in Post, where: like(p.blob, ^<<0, 1, 2>>), select: p.id)
  end

  test "text type as string" do
    assert %Post{} = post = TestRepo.insert!(%Post{id: 1, blob: "hello"})
    id = post.id
    assert post.blob == "hello"
    assert [^id] = TestRepo.all(from p in Post, where: like(p.blob, ^"hello"), select: p.id)
  end

  test "array type" do
    ints = [1, 2, 3]
    _tag = TestRepo.insert!(%Tag{ints: ints})

    assert TestRepo.all(from t in Tag, where: t.ints == ^[], select: t.ints) == []
    assert TestRepo.all(from t in Tag, where: t.ints == ^[1, 2, 3], select: t.ints) == [ints]

    # Both sides interpolation
    assert TestRepo.all(from t in Tag, where: ^"b" in ^["a", "b", "c"], select: t.ints) == [ints]

    assert TestRepo.all(from t in Tag, where: ^"b" in [^"a", ^"b", ^"c"], select: t.ints) == [
             ints
           ]

    # Querying
    assert TestRepo.all(from t in Tag, where: t.ints == [1, 2, 3], select: t.ints) == [ints]
    assert TestRepo.all(from t in "tags", where: t.ints == [1, 2, 3], select: t.ints) == [ints]

    # ClickHouse doesn't support IN operator on array columns
    # works: select 1 in [1,2,3]
    # fails: select * from tags t where 0 in t.ints

    assert_raise Ch.Error, ~r/UNKNOWN_TABLE/, fn ->
      TestRepo.all(from t in Tag, where: 0 in t.ints, select: t.ints)
    end

    assert_raise Ch.Error, ~r/UNKNOWN_TABLE/, fn ->
      TestRepo.all(from t in Tag, where: 1 in t.ints, select: t.ints)
    end

    assert_raise Ch.Error, ~r/UNKNOWN_TABLE/, fn ->
      TestRepo.all(from t in Tag, where: ^0 in t.ints, select: t.ints)
    end

    assert_raise Ch.Error, ~r/UNKNOWN_TABLE/, fn ->
      TestRepo.all(from t in Tag, where: ^1 in t.ints, select: t.ints)
    end

    # has(arr, el) can be used instead

    assert TestRepo.all(from t in Tag, where: fragment("has(?, ?)", t.ints, 0), select: t.ints) ==
             []

    assert TestRepo.all(from t in Tag, where: fragment("has(?, ?)", t.ints, 1), select: t.ints) ==
             [ints]

    assert TestRepo.all(from t in Tag, where: fragment("has(?, ?)", t.ints, ^0), select: t.ints) ==
             []

    assert TestRepo.all(from t in Tag, where: fragment("has(?, ?)", t.ints, ^1), select: t.ints) ==
             [ints]

    # # Update
    # tag = TestRepo.update!(Ecto.Changeset.change tag, ints: nil)
    # assert TestRepo.get!(Tag, tag.id).ints == nil

    # tag = TestRepo.update!(Ecto.Changeset.change tag, ints: [3, 2, 1])
    # assert TestRepo.get!(Tag, tag.id).ints == [3, 2, 1]

    # # Update all
    # {1, _} = TestRepo.update_all(Tag, push: [ints: 0])
    # assert TestRepo.get!(Tag, tag.id).ints == [3, 2, 1, 0]

    # {1, _} = TestRepo.update_all(Tag, pull: [ints: 2])
    # assert TestRepo.get!(Tag, tag.id).ints == [3, 1, 0]

    # {1, _} = TestRepo.update_all(Tag, set: [ints: nil])
    # assert TestRepo.get!(Tag, tag.id).ints == nil
  end

  test "array type with custom types" do
    uuids = ["51fcfbdd-ad60-4ccb-8bf9-47aabd66d075"]
    TestRepo.insert!(%Tag{uuids: ["51fcfbdd-ad60-4ccb-8bf9-47aabd66d075"]})

    assert TestRepo.all(from t in Tag, where: t.uuids == ^[], select: t.uuids) == []

    # TODO t.uuids == ^["51fcfbdd-ad60-4ccb-8bf9-47aabd66d075"]
    # need to cast the rhs to Array(UUID)
    assert TestRepo.all(
             from t in Tag,
               where:
                 t.uuids == type(^["51fcfbdd-ad60-4ccb-8bf9-47aabd66d075"], {:array, Ecto.UUID}),
               select: t.uuids
           ) == [uuids]

    # TODO add support for alter_update_all
    # {1, _} = TestRepo.update_all(Tag, set: [uuids: nil])

    TestRepo.query!("alter table tags update uuids = [] where 1", [],
      settings: [mutations_sync: 1]
    )

    assert TestRepo.all(from t in Tag, select: t.uuids) == [[]]
  end

  test "array type with nil in array" do
    tag = TestRepo.insert!(%Tag{id: 1, ints: [1, nil, 3]})
    assert tag.ints == [1, nil, 3]
    assert tag = TestRepo.reload!(tag)
    assert tag.ints == [1, 0, 3]
  end

  describe "unsopperted map" do
    @describetag skip: true
    test "untyped map"
    test "typed string map"
    test "typed float map"
    test "map type on update"
    test "embeds one"
    test "json_extract_path with primitive values"
    test "json_extract_path with arrays and objects"
    test "json_extract_path with embeds"
    test "json_extract_path with custom field source"
    test "embeds one with custom type"
    test "empty embeds one"
    test "embeds many"
    test "empty embeds many"
    test "nested embeds"
  end

  test "decimal type" do
    decimal = Decimal.new("1.0")
    TestRepo.insert!(%Post{cost: decimal})

    [cost] = TestRepo.all(from p in Post, where: p.cost == ^decimal, select: p.cost)
    assert Decimal.equal?(decimal, cost)
    [cost] = TestRepo.all(from p in Post, where: p.cost == ^1.0, select: p.cost)
    assert Decimal.equal?(decimal, cost)
    [cost] = TestRepo.all(from p in Post, where: p.cost == ^1, select: p.cost)
    assert Decimal.equal?(decimal, cost)
    [cost] = TestRepo.all(from p in Post, where: p.cost == 1.0, select: p.cost)
    assert Decimal.equal?(decimal, cost)
    [cost] = TestRepo.all(from p in Post, where: p.cost == 1, select: p.cost)
    assert Decimal.equal?(decimal, cost)
    [cost] = TestRepo.all(from p in Post, select: p.cost * 2)
    assert Decimal.equal?(Decimal.new("2.0"), cost)
    [cost] = TestRepo.all(from p in Post, select: p.cost - p.cost)
    assert Decimal.equal?(Decimal.new("0.0"), cost)
  end

  @float32 Ecto.ParameterizedType.init(Ch, type: "Float32")
  @decimal64_2 Ecto.ParameterizedType.init(Ch, type: "Decimal64(2)")
  test "decimal typed aggregations" do
    decimal = Decimal.new("1.0")
    TestRepo.insert!(%Post{cost: decimal})

    assert [1] = TestRepo.all(from p in Post, select: type(sum(p.cost), :integer))
    assert [1.0] = TestRepo.all(from p in Post, select: type(sum(p.cost), ^@float32))
    [cost] = TestRepo.all(from p in Post, select: type(sum(p.cost), ^@decimal64_2))
    assert Decimal.equal?(decimal, cost)
  end

  test "on coalesce with mixed types" do
    decimal = Decimal.new("1.0")
    TestRepo.insert!(%Post{cost: decimal})
    [cost] = TestRepo.all(from p in Post, select: coalesce(p.cost, 0))
    assert Decimal.equal?(decimal, cost)
  end

  @tag skip: true
  test "unions with literals" do
    TestRepo.insert!(%Post{})
    TestRepo.insert!(%Post{})

    query1 = from(p in Post, select: %{n: 1})
    query2 = from(p in Post, select: %{n: 2})

    assert TestRepo.all(union_all(query1, ^query2)) ==
             [%{n: 1}, %{n: 1}, %{n: 2}, %{n: 2}]

    query1 = from(p in Post, select: %{n: 1.0})
    query2 = from(p in Post, select: %{n: 2.0})

    assert TestRepo.all(union_all(query1, ^query2)) ==
             [%{n: 1.0}, %{n: 1.0}, %{n: 2.0}, %{n: 2.0}]

    query1 = from(p in Post, select: %{n: "foo"})
    query2 = from(p in Post, select: %{n: "bar"})

    assert TestRepo.all(union_all(query1, ^query2)) ==
             [%{n: "foo"}, %{n: "foo"}, %{n: "bar"}, %{n: "bar"}]
  end

  test "schemaless types" do
    TestRepo.insert!(%Post{visits: 123})
    assert [123] = TestRepo.all(from p in "posts", select: type(p.visits, :integer))
  end

  @tag skip: true
  test "schemaless calendar types" do
    datetime = ~N[2014-01-16 20:26:51]
    assert {1, _} = TestRepo.insert_all("posts", [[inserted_at: datetime]])
    # assert {1, _} = TestRepo.update_all("posts", set: [inserted_at: datetime])

    assert [_] =
             TestRepo.all(
               from p in "posts", where: p.inserted_at >= ^datetime, select: p.inserted_at
             )

    assert [_] =
             TestRepo.all(
               from p in "posts", where: p.inserted_at in [^datetime], select: p.inserted_at
             )

    assert [_] =
             TestRepo.all(
               from p in "posts", where: p.inserted_at in ^[datetime], select: p.inserted_at
             )
  end
end
