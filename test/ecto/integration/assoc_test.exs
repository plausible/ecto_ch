defmodule Ecto.Integration.AssocTest do
  use Ecto.Integration.Case

  alias Ecto.Integration.TestRepo
  alias Ecto.Integration.{Post, Comment, Permalink}

  test "has_many assoc" do
    p1 = TestRepo.insert!(%Post{title: "1"})
    p2 = TestRepo.insert!(%Post{title: "2"})

    %Comment{id: cid1} = TestRepo.insert!(%Comment{text: "1", post_id: p1.id})
    %Comment{id: cid2} = TestRepo.insert!(%Comment{text: "2", post_id: p1.id})
    %Comment{id: cid3} = TestRepo.insert!(%Comment{text: "3", post_id: p2.id})

    [c1, c2] = TestRepo.all(Ecto.assoc(p1, :comments)) |> Enum.sort_by(& &1.id)
    assert c1.id == cid1
    assert c2.id == cid2

    [c1, c2, c3] = TestRepo.all(Ecto.assoc([p1, p2], :comments)) |> Enum.sort_by(& &1.id)
    assert c1.id == cid1
    assert c2.id == cid2
    assert c3.id == cid3
  end

  @tag skip: true
  test "has_one assoc" do
    p1 = TestRepo.insert!(%Post{title: "1"})
    p2 = TestRepo.insert!(%Post{title: "2"})

    %Permalink{id: lid1} = TestRepo.insert!(%Permalink{url: "1", post_id: p1.id})

    %Permalink{} = TestRepo.insert!(%Permalink{url: "2"})
    %Permalink{id: lid3} = TestRepo.insert!(%Permalink{url: "3", post_id: p2.id})

    [l1, l3] = TestRepo.all(Ecto.assoc([p1, p2], :permalink))
    assert l1.id == lid1
    assert l3.id == lid3
  end
end
