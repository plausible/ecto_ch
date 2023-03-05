defmodule Ecto.Integration.JoinsTest do
  use Ecto.Integration.Case
  import Ecto.Query

  alias Ecto.Integration.TestRepo
  alias Ecto.Integration.{Post, Permalink, Comment}

  test "joins" do
    _p = TestRepo.insert!(%Post{title: "1"})
    p2 = TestRepo.insert!(%Post{title: "2"})
    c1 = TestRepo.insert!(%Permalink{url: "1", post_id: p2.id})

    query =
      from p in Post,
        join: c in assoc(p, :permalink),
        order_by: p.id,
        select: {p, c}

    assert [{%Post{}, %Permalink{}}] = TestRepo.all(query)

    query =
      from p in Post,
        join: c in assoc(p, :permalink),
        on: c.id == ^c1.id,
        select: {p, c}

    assert [{%Post{}, %Permalink{}}] = TestRepo.all(query)
  end

  test "joins with queries" do
    _p = TestRepo.insert!(%Post{title: "1"})
    p2 = TestRepo.insert!(%Post{title: "2"})
    _c = TestRepo.insert!(%Permalink{url: "1", post_id: p2.id})

    # Joined query without parameter
    permalink = from c in Permalink, where: c.url == "1"

    query =
      from p in Post,
        join: c in ^permalink,
        on: c.post_id == p.id,
        select: {p, c}

    assert [{%Post{}, %Permalink{}}] = TestRepo.all(query)

    # Joined query with parameter
    # permalink = from c in Permalink, where: c.url == "1"

    # query =
    #   from p in Post,
    #     join: c in ^permalink,
    #     on: c.id == ^c1.id,
    #     order_by: p.title,
    #     select: {p, c}

    # assert [{%Post{}, %Permalink{}}, {%Post{}, %Permalink{}}] =
    #          TestRepo.all(query)
  end

  test "named joins" do
    _p = TestRepo.insert!(%Post{title: "1"})
    p2 = TestRepo.insert!(%Post{title: "2"})
    _c = TestRepo.insert!(%Permalink{url: "1", post_id: p2.id})

    query =
      from(p in Post,
        join: c in assoc(p, :permalink),
        as: :permalink,
        order_by: p.id
      )
      |> select([p, permalink: c], {p, c})

    assert [{%Post{}, %Permalink{}}] = TestRepo.all(query)
  end

  test "joins with dynamic in :on" do
    p = TestRepo.insert!(%Post{title: "1"})
    _c = TestRepo.insert!(%Permalink{url: "1", post_id: p.id})

    # join_on = dynamic([p, ..., c], c.id == ^c.id)
    join_on = dynamic([p, ..., c], c.post_id == p.id)

    query =
      from(p in Post,
        join: c in Permalink,
        on: ^join_on
      )
      |> select([p, c], {p, c})

    assert [{%Post{}, %Permalink{}}] = TestRepo.all(query)

    # join_on = dynamic([p, permalink: c], c.id == ^c.id)
    join_on = dynamic([p, permalink: c], c.post_id == p.id)

    query =
      from(p in Post,
        join: c in Permalink,
        as: :permalink,
        on: ^join_on
      )
      |> select([p, c], {p, c})

    assert [{%Post{}, %Permalink{}}] = TestRepo.all(query)
  end

  test "cross joins with missing entries" do
    _p = TestRepo.insert!(%Post{title: "1"})
    p2 = TestRepo.insert!(%Post{title: "2"})
    _c = TestRepo.insert!(%Permalink{url: "1", post_id: p2.id})

    query =
      from(p in Post,
        cross_join: c in Permalink,
        order_by: p.id,
        select: {p, c}
      )

    assert [{%Post{}, %Permalink{}}, {%Post{}, %Permalink{}}] = TestRepo.all(query)
  end

  test "left joins with missing entries" do
    _p = TestRepo.insert!(%Post{title: "1"})
    p2 = TestRepo.insert!(%Post{title: "2"})
    _c = TestRepo.insert!(%Permalink{url: "1", post_id: p2.id})

    query =
      from(p in Post,
        left_join: c in assoc(p, :permalink),
        order_by: p.id,
        select: {p, c}
      )

    # TODO permalink with id = 0 should be nil
    # assert [{^p1, nil}, {^p2, ^c1}] = TestRepo.all(query)
    assert [{%Post{}, %Permalink{id: 0}}, {%Post{}, %Permalink{}}] = TestRepo.all(query)
  end

  test "left join with missing entries from subquery" do
    _p = TestRepo.insert!(%Post{title: "1"})
    p2 = TestRepo.insert!(%Post{title: "2"})
    _c = TestRepo.insert!(%Permalink{url: "1", post_id: p2.id})

    query =
      from(p in Post,
        left_join: c in subquery(Permalink),
        on: p.id == c.post_id,
        order_by: p.id,
        select: {p, c}
      )

    assert [{%Post{}, %Permalink{id: 0}}, {%Post{}, %Permalink{}}] = TestRepo.all(query)
  end

  test "right joins with missing entries" do
    %Post{id: pid1} = TestRepo.insert!(%Post{title: "1"})
    %Post{id: pid2} = TestRepo.insert!(%Post{title: "2"})

    %Permalink{id: plid1} = TestRepo.insert!(%Permalink{url: "1", post_id: pid2})

    TestRepo.insert!(%Comment{text: "1", post_id: pid1})
    TestRepo.insert!(%Comment{text: "2", post_id: pid2})
    TestRepo.insert!(%Comment{text: "3", post_id: nil})

    query =
      from(p in Post,
        right_join: c in assoc(p, :comments),
        preload: :permalink,
        order_by: c.id
      )

    assert [p1, p2, p3] = TestRepo.all(query)
    assert p1.id == pid1
    assert p2.id == pid2
    assert p3.id == 0

    assert p1.permalink == nil
    assert p2.permalink.id == plid1
  end

  # TODO
  ## Associations joins
  ## Association preload
  ## Nested
end
