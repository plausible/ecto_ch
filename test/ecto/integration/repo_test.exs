defmodule Ecto.Integration.RepoTest do
  use Ecto.Integration.Case
  import Ecto.Query

  alias Ecto.Integration.TestRepo
  alias Ecto.Integration.{Post, Permalink, User, Comment}

  test "returns already started for started repos" do
    assert {:error, {:already_started, _}} = TestRepo.start_link()
  end

  test "supports unnamed repos" do
    assert {:ok, pid} = TestRepo.start_link(name: nil)
    assert Ecto.Repo.Queryable.all(pid, Post, Ecto.Repo.Supervisor.tuplet(pid, [])) == []
  end

  test "all empty" do
    assert TestRepo.all(Post) == []
    assert TestRepo.all(from(p in Post)) == []
  end

  test "all with in" do
    TestRepo.insert!(%Post{title: "hello"})

    # Works without the query cache.
    assert_raise Ecto.Query.CastError, fn ->
      TestRepo.all(from p in Post, where: p.title in ^nil)
    end

    assert [] = TestRepo.all(from p in Post, where: p.title in [])
    assert [] = TestRepo.all(from p in Post, where: p.title in ["1", "2", "3"])
    assert [] = TestRepo.all(from p in Post, where: p.title in ^[])

    assert [_] = TestRepo.all(from p in Post, where: p.title not in [])
    assert [_] = TestRepo.all(from p in Post, where: p.title in ["1", "hello", "3"])
    assert [_] = TestRepo.all(from p in Post, where: p.title in ["1", ^"hello", "3"])
    assert [_] = TestRepo.all(from p in Post, where: p.title in ^["1", "hello", "3"])

    # Still doesn't work after the query cache.
    assert_raise Ecto.Query.CastError, fn ->
      TestRepo.all(from p in Post, where: p.title in ^nil)
    end
  end

  test "all using named from" do
    TestRepo.insert!(%Post{title: "hello"})

    query =
      from(p in Post, as: :post)
      |> where([post: p], p.title == "hello")

    assert [_] = TestRepo.all(query)
  end

  test "all without schema" do
    %Post{} = TestRepo.insert!(%Post{title: "title1"})
    %Post{} = TestRepo.insert!(%Post{title: "title2"})

    assert ["title1", "title2"] =
             TestRepo.all(from(p in "posts", order_by: p.title, select: p.title))

    assert [_] = TestRepo.all(from(p in "posts", where: p.title == "title1", select: p.id))
  end

  test "all shares metadata" do
    TestRepo.insert!(%Post{title: "title1"})
    TestRepo.insert!(%Post{title: "title2"})

    [post1, post2] = TestRepo.all(Post)
    assert :erts_debug.same(post1.__meta__, post2.__meta__)

    [new_post1, new_post2] = TestRepo.all(Post)
    assert :erts_debug.same(post1.__meta__, new_post1.__meta__)
    assert :erts_debug.same(post2.__meta__, new_post2.__meta__)
  end

  test "all with invalid prefix" do
    assert catch_error(TestRepo.all("posts", prefix: "oops"))
  end

  # TODO why tx
  @tag skip: true
  test "insert, update and delete" do
    post = %Post{title: "insert, update, delete", visits: 1}
    meta = post.__meta__

    assert %Post{} = inserted = TestRepo.insert!(post)
    # assert %Post{} = updated = TestRepo.update!(Ecto.Changeset.change(inserted, visits: 2))

    deleted_meta = put_in(meta.state, :deleted)

    assert %Post{__meta__: ^deleted_meta} =
             TestRepo.delete!(inserted,
               settings: [
                 allow_experimental_lightweight_delete: 1,
                 mutations_sync: 1
               ]
             )

    loaded_meta = put_in(meta.state, :loaded)
    assert %Post{__meta__: ^loaded_meta} = TestRepo.insert!(post)

    post = TestRepo.one(Post)
    assert post.__meta__.state == :loaded
    assert post.inserted_at
  end

  test "insert, update and delete with field source" do
    permalink = %Permalink{url: "url"}
    assert %Permalink{url: "url"} = inserted = TestRepo.insert!(permalink)

    # assert %Permalink{url: "new"} =
    #          updated = TestRepo.update!(Ecto.Changeset.change(inserted, url: "new"))

    assert %Permalink{url: "url"} =
             TestRepo.delete!(inserted,
               settings: [
                 allow_experimental_lightweight_delete: 1,
                 mutations_sync: 1
               ]
             )
  end

  @tag skip: true
  test "insert, update and delete with composite pk"
  @tag skip: true
  test "insert, update and delete with associated composite pk"

  # TODO why tx
  @tag capture_log: true
  test "insert, update and delete with invalid prefix" do
    post = TestRepo.insert!(%Post{})
    changeset = Ecto.Changeset.change(post, title: "foo")
    assert catch_error(TestRepo.insert(%Post{}, prefix: "oops"))
    # assert catch_error(TestRepo.update(changeset, prefix: "oops"))
    assert catch_error(TestRepo.delete(changeset, prefix: "oops"))

    # Check we can still insert the post after the invalid prefix attempt
    assert %Post{} = TestRepo.insert!(%Post{})
  end

  test "insert and update with changeset" do
    # On insert we merge the fields and changes
    changeset =
      Ecto.Changeset.cast(
        %Post{visits: 13, title: "wrong"},
        %{"title" => "hello", "temp" => "unknown"},
        ~w(title temp)a
      )

    post = TestRepo.insert!(changeset)
    assert %Post{visits: 13, title: "hello", temp: "unknown"} = post
    assert %Post{visits: 13, title: "hello", temp: "temp"} = TestRepo.get!(Post, post.id)

    # On update we merge only fields, direct schema changes are discarded
    # changeset = Ecto.Changeset.cast(%{post | visits: 17},
    #                                 %{"title" => "world", "temp" => "unknown"}, ~w(title temp)a)

    # assert %Post{visits: 17, title: "world", temp: "unknown"} = TestRepo.update!(changeset)
    # assert %Post{visits: 13, title: "world", temp: "temp"} = TestRepo.get!(Post, post.id)
  end

  test "insert and update with empty changeset" do
    # On insert we merge the fields and changes
    changeset = Ecto.Changeset.cast(%Permalink{}, %{}, ~w())
    assert %Permalink{} = _permalink = TestRepo.insert!(changeset)

    # Assert we can update the same value twice,
    # without changes, without triggering stale errors.
    # changeset = Ecto.Changeset.cast(permalink, %{}, ~w())
    # assert TestRepo.update!(changeset) == permalink
    # assert TestRepo.update!(changeset) == permalink
  end

  # TODO
  # test "insert with no primary key" do
  #   assert %Barebone{num: nil} = TestRepo.insert!(%Barebone{})
  #   assert %Barebone{num: 13} = TestRepo.insert!(%Barebone{num: 13})
  # end

  @tag skip: true
  test "insert and update with changeset read after writes"

  test "insert autogenerates for custom type" do
    post = TestRepo.insert!(%Post{uuid: nil})
    assert byte_size(post.uuid) == 36
    assert TestRepo.get_by!(Post, uuid: post.uuid).id == post.id
  end

  @tag skip: true
  test "insert autogenerates for custom id type"

  test "insert with user-assigned primary key" do
    assert %Post{} = TestRepo.insert!(%Post{})
  end

  @tag skip: true
  test "insert and update with user-assigned primary key in changeset" do
    changeset = Ecto.Changeset.cast(%Post{}, %{"id" => "13"}, ~w(id)a)
    assert %Post{} = _post = TestRepo.insert!(changeset)

    # changeset = Ecto.Changeset.cast(post, %{"id" => "15"}, ~w(id)a)
    # assert %Post{5} = TestRepo.update!(changeset)
  end

  @tag skip: true
  test "insert and fetch a schema with utc timestamps" do
    datetime = DateTime.from_unix!(System.os_time(:second), :second)
    TestRepo.insert!(%User{inserted_at: datetime})
    assert [%{inserted_at: ^datetime}] = TestRepo.all(User)
  end

  @tag skip: true
  test "optimistic locking in update/delete operations"
  @tag skip: true
  test "optimistic locking in update operation with nil field"
  @tag skip: true
  test "optimistic locking in delete operation with nil field"
  @tag skip: true
  test "unique constraint"
  @tag skip: true
  test "unique constraint from association"
  @tag skip: true
  test "unique constraint with binary_id"
  @tag skip: true
  test "unique pseudo-constraint violation error message with join table at the repository"
  @tag skip: true
  test "unique constraint violation error message with join table in single changeset"
  @tag skip: true
  test "unique constraint violation error message with join table and separate changesets"
  @tag skip: true
  test "foreign key constraint"
  @tag skip: true
  test "assoc constraint"
  @tag skip: true
  test "no assoc constraint error"
  @tag skip: true
  test "no assoc constraint with changeset mismatch"
  @tag skip: true
  test "no assoc constraint with changeset match"
  @tag skip: true
  test "insert and update with embeds during failing child foreign key"
  @tag skip: true
  test "unsafe_validate_unique/4"
  @tag skip: true
  test "unsafe_validate_unique/4 with composite keys"

  test "get(!)" do
    post1 = TestRepo.insert!(%Post{title: "1"})
    post2 = TestRepo.insert!(%Post{title: "2"})

    assert %Post{title: "1"} = TestRepo.get(Post, post1.id)
    # With casting
    assert %Post{title: "2"} = TestRepo.get(Post, to_string(post2.id))

    assert %Post{title: "1"} = TestRepo.get!(Post, post1.id)
    # With casting
    assert %Post{title: "2"} = TestRepo.get!(Post, to_string(post2.id))

    TestRepo.delete!(post1,
      settings: [allow_experimental_lightweight_delete: 1, mutations_sync: 1]
    )

    assert TestRepo.get(Post, post1.id) == nil

    assert_raise Ecto.NoResultsError, fn ->
      TestRepo.get!(Post, post1.id)
    end
  end

  @tag skip: true
  test "get(!) with custom source" do
    # custom = Ecto.put_meta(%Custom{}, source: "posts")
    # custom = TestRepo.insert!(custom)
    # bid    = custom.bid
    # assert %Custom{bid: ^bid, __meta__: %{source: "posts"}} =
    #        TestRepo.get(from(c in {"posts", Custom}), bid)
  end

  @tag skip: true
  test "get(!) with binary_id" do
    # custom = TestRepo.insert!(%Custom{})
    # bid = custom.bid
    # assert %Custom{bid: ^bid} = TestRepo.get(Custom, bid)
  end

  test "get_by(!)" do
    post1 = TestRepo.insert!(%Post{title: "1", visits: 1})
    post2 = TestRepo.insert!(%Post{title: "2", visits: 2})

    assert %Post{title: "1", visits: 1} = TestRepo.get_by(Post, id: post1.id)
    assert %Post{title: "1", visits: 1} = TestRepo.get_by(Post, title: post1.title)

    assert %Post{title: "1", visits: 1} = TestRepo.get_by(Post, id: post1.id, title: post1.title)

    # With casting
    assert %Post{title: "2", visits: 2} = TestRepo.get_by(Post, id: to_string(post2.id))
    assert nil == TestRepo.get_by(Post, title: "hey")
    assert nil == TestRepo.get_by(Post, id: post2.id, visits: 3)

    assert %Post{title: "1", visits: 1} = TestRepo.get_by!(Post, id: post1.id)
    assert %Post{title: "1", visits: 1} = TestRepo.get_by!(Post, title: post1.title)
    assert %Post{title: "1", visits: 1} = TestRepo.get_by!(Post, id: post1.id, visits: 1)
    # With casting
    assert %Post{title: "2", visits: 2} = TestRepo.get_by!(Post, id: to_string(post2.id))

    assert %Post{title: "1", visits: 1} = TestRepo.get_by!(Post, %{id: post1.id})

    assert_raise Ecto.NoResultsError, fn ->
      TestRepo.get_by!(Post, id: post2.id, title: "hey")
    end
  end

  test "reload" do
    post1 = TestRepo.insert!(%Post{title: "1", visits: 1})
    post2 = TestRepo.insert!(%Post{title: "2", visits: 2})

    assert %Post{title: "1", visits: 1} = TestRepo.reload(post1)

    assert [%Post{title: "1", visits: 1}, %Post{title: "2", visits: 2}] =
             TestRepo.reload([post1, post2])

    assert [%Post{title: "1", visits: 1}, %Post{title: "2", visits: 2}, nil] =
             TestRepo.reload([post1, post2, %Post{id: 0}])

    assert nil == TestRepo.reload(%Post{id: 0})

    # keeps order as received in the params
    assert [%Post{title: "2", visits: 2}, %Post{title: "1", visits: 1}] =
             TestRepo.reload([post2, post1])

    # TestRepo.update_all(Post, inc: [visits: 1])
    TestRepo.query!("alter table posts update visits = visits + 1 where 1", [],
      settings: [mutations_sync: 1]
    )

    assert [%{visits: 2}, %{visits: 3}] = TestRepo.reload([post1, post2])
  end

  test "reload ignores preloads" do
    post = TestRepo.insert!(%Post{title: "1", visits: 1}) |> TestRepo.preload(:comments)

    assert %{comments: %Ecto.Association.NotLoaded{}} = TestRepo.reload(post)
  end

  test "reload!" do
    post1 = TestRepo.insert!(%Post{title: "1", visits: 1})
    post2 = TestRepo.insert!(%Post{title: "2", visits: 2})

    assert post1 == TestRepo.reload!(post1)
    assert [post1, post2] == TestRepo.reload!([post1, post2])

    assert_raise RuntimeError, ~r"could not reload", fn ->
      TestRepo.reload!([post1, post2, %Post{id: -1}])
    end

    assert_raise Ecto.NoResultsError, fn ->
      TestRepo.reload!(%Post{id: -1})
    end

    assert [post2, post1] == TestRepo.reload([post2, post1])

    # TestRepo.update_all(Post, inc: [visits: 1])
    TestRepo.query!("alter table posts update visits = visits + 1 where 1", [],
      settings: [mutations_sync: 1]
    )

    assert [%{visits: 2}, %{visits: 3}] = TestRepo.reload!([post1, post2])
  end

  test "first, last and one(!)" do
    post1 = TestRepo.insert!(%Post{title: "1"})
    post2 = TestRepo.insert!(%Post{title: "2"})

    assert post1 == Post |> first |> TestRepo.one()
    assert post2 == Post |> last |> TestRepo.one()

    query = from p in Post, order_by: p.title
    assert post1 == query |> first |> TestRepo.one()
    assert post2 == query |> last |> TestRepo.one()

    query = from p in Post, order_by: [desc: p.title], limit: 10
    assert post2 == query |> first |> TestRepo.one()
    assert post1 == query |> last |> TestRepo.one()

    query = from p in Post, where: is_nil(p.id)
    refute query |> first |> TestRepo.one()
    refute query |> last |> TestRepo.one()
    assert_raise Ecto.NoResultsError, fn -> query |> first |> TestRepo.one!() end
    assert_raise Ecto.NoResultsError, fn -> query |> last |> TestRepo.one!() end
  end

  test "exists?" do
    TestRepo.insert!(%Post{title: "1", visits: 2})
    TestRepo.insert!(%Post{title: "2", visits: 1})

    query = from p in Post, where: not is_nil(p.title), limit: 2
    assert query |> TestRepo.exists?() == true

    query = from p in Post, where: p.title == "1", select: p.title
    assert query |> TestRepo.exists?() == true

    query = from p in Post, where: is_nil(p.id)
    assert query |> TestRepo.exists?() == false

    query = from p in Post, where: is_nil(p.id)
    assert query |> TestRepo.exists?() == false

    query =
      from(p in Post,
        select: {p.visits, avg(p.visits)},
        group_by: p.visits,
        having: avg(p.visits) > 1
      )

    assert query |> TestRepo.exists?() == true
  end

  test "aggregate" do
    assert TestRepo.aggregate(Post, :max, :visits) == 0

    TestRepo.insert!(%Post{visits: 10})
    TestRepo.insert!(%Post{visits: 12})
    TestRepo.insert!(%Post{visits: 14})
    TestRepo.insert!(%Post{visits: 14})

    # Barebones
    assert TestRepo.aggregate(Post, :max, :visits) == 14
    assert TestRepo.aggregate(Post, :min, :visits) == 10
    assert TestRepo.aggregate(Post, :count, :visits) == 4
    assert "50" = to_string(TestRepo.aggregate(Post, :sum, :visits))

    # With order_by
    query = from Post, order_by: [asc: :visits]
    assert TestRepo.aggregate(query, :max, :visits) == 14

    # With order_by and limit
    query = from Post, order_by: [asc: :visits], limit: 2
    assert TestRepo.aggregate(query, :max, :visits) == 12
  end

  test "aggregate avg" do
    TestRepo.insert!(%Post{visits: 10})
    TestRepo.insert!(%Post{visits: 12})
    TestRepo.insert!(%Post{visits: 14})
    TestRepo.insert!(%Post{visits: 14})

    assert "12.5" <> _ = to_string(TestRepo.aggregate(Post, :avg, :visits))
  end

  test "aggregate with distinct" do
    TestRepo.insert!(%Post{visits: 10})
    TestRepo.insert!(%Post{visits: 12})
    TestRepo.insert!(%Post{visits: 14})
    TestRepo.insert!(%Post{visits: 14})

    query = from Post, order_by: [asc: :visits], distinct: true
    assert TestRepo.aggregate(query, :count, :visits) == 3
  end

  @tag skip: true
  test "insert all" do
    # TODO default: 1 for lock_version doesn't work

    assert {2, nil} =
             TestRepo.insert_all("comments", [[text: "1"], %{text: "2", lock_version: 2}],
               types: [text: :string, lock_version: :u8]
             )

    assert {2, nil} =
             TestRepo.insert_all(
               {"comments", Comment},
               [
                 [text: "3"],
                 %{text: "4", lock_version: 2}
               ],
               types: [text: :string, lock_version: :u8]
             )

    assert [
             %Comment{text: "1", lock_version: 1},
             %Comment{text: "2", lock_version: 2},
             %Comment{text: "3", lock_version: 1},
             %Comment{text: "4", lock_version: 2}
           ] = TestRepo.all(Comment)

    assert {2, nil} = TestRepo.insert_all(Post, [[], []])
    assert [%Post{}, %Post{}] = TestRepo.all(Post)

    assert {0, nil} = TestRepo.insert_all("posts", [])
    assert {0, nil} = TestRepo.insert_all({"posts", Post}, [])
  end

  @tag skip: true
  test "insert all with query for single fields" do
    comment = TestRepo.insert!(%Comment{text: "1", lock_version: 1})

    text_query =
      from c in Comment,
        select: c.text,
        where: [id: ^comment.id, lock_version: 1]

    lock_version_query =
      from c in Comment,
        select: c.lock_version,
        where: [id: ^comment.id]

    rows = [
      [text: "2", lock_version: lock_version_query],
      [lock_version: lock_version_query, text: "3"],
      [text: text_query],
      [text: text_query, lock_version: lock_version_query],
      [lock_version: 6, text: "6"]
    ]

    assert {5, nil} = TestRepo.insert_all(Comment, rows, [])

    inserted_rows =
      Comment
      |> where([c], c.id != ^comment.id)
      |> TestRepo.all()

    assert [
             %Comment{text: "2", lock_version: 1},
             %Comment{text: "3", lock_version: 1},
             %Comment{text: "1"},
             %Comment{text: "1", lock_version: 1},
             %Comment{text: "6", lock_version: 6}
           ] = inserted_rows
  end

  describe "insert_all with source query" do
    test "insert_all with query and conflict target" do
      {:ok, %Post{id: id}} =
        TestRepo.insert(%Post{
          title: "A generic title"
        })

      source =
        from p in Post,
          select: %{
            title:
              fragment("concat(?, ?, toString(?))", p.title, type(^" suffix ", :string), p.id)
          }

      assert {1, _} =
               TestRepo.insert_all(Post, source, conflict_target: [:id], on_conflict: :replace_all)

      expected_title = "A generic title suffix #{id}"

      # TODO
      assert %Post{title: ^expected_title} = TestRepo.get(Post, 0)
    end

    test "insert_all with query and returning" do
      {:ok, %Post{id: _id}} =
        TestRepo.insert(%Post{
          title: "A generic title"
        })

      source =
        from p in Post,
          select: %{
            title: fragment("concat(?, ?, ?)", p.title, type(^" suffix ", :string), p.id)
          }

      assert_raise ArgumentError,
                   "ClickHouse does not support RETURNING on INSERT statements",
                   fn -> TestRepo.insert_all(Post, source, returning: [:id, :title]) end
    end
  end
end
