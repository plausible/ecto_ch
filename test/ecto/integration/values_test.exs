defmodule Ecto.Integration.ValuesTest do
  use Ecto.Integration.Case, async: true
  import Ecto.Query
  alias Ecto.Integration.TestRepo

  test "it works" do
    values = [%{id: 1, text: "abc"}, %{id: 2, text: "xyz"}]
    types = %{id: :integer, text: :string}

    query =
      from v1 in values(values, types),
        select: %{id: v1.id, text: v1.text}

    assert TestRepo.to_sql(:all, query) ==
             {
               ~s[SELECT v0."id",v0."text" FROM VALUES('id Int64,text String',({$0:Int64},{$1:String}),({$2:Int64},{$3:String})) AS v0],
               [1, "abc", 2, "xyz"]
             }

    assert TestRepo.to_inline_sql(:all, query) ==
             ~s[SELECT v0."id",v0."text" FROM VALUES('id Int64,text String',(1,'abc'),(2,'xyz')) AS v0]

    assert TestRepo.all(query) == [
             %{id: 1, text: "abc"},
             %{id: 2, text: "xyz"}
           ]

    query =
      from v1 in values(values, types),
        join: v2 in values(values, types),
        on: v1.id == v2.id

    assert TestRepo.to_sql(:all, query) ==
             {"""
              SELECT v0."id",v0."text" FROM VALUES('id Int64,text String',({$0:Int64},{$1:String}),({$2:Int64},{$3:String})) AS v0 \
              INNER JOIN VALUES('id Int64,text String',({$4:Int64},{$5:String}),({$6:Int64},{$7:String})) AS v1 ON v0."id" = v1."id"\
              """, [1, "abc", 2, "xyz", 1, "abc", 2, "xyz"]}

    assert TestRepo.to_inline_sql(:all, query) == """
           SELECT v0."id",v0."text" FROM VALUES('id Int64,text String',(1,'abc'),(2,'xyz')) AS v0 \
           INNER JOIN VALUES('id Int64,text String',(1,'abc'),(2,'xyz')) AS v1 ON v0."id" = v1."id"\
           """

    assert TestRepo.all(query) == [
             %{id: 1, text: "abc"},
             %{id: 2, text: "xyz"}
           ]
  end
end
