defmodule Ecto.Integration.SpliceTest do
  use Ecto.Integration.Case, async: true
  import Ecto.Query
  alias Ecto.Integration.TestRepo

  # https://github.com/plausible/ecto_ch/issues/239
  test "it works" do
    values = ["one", "two", "three", "four"]

    query =
      from e in fragment("VALUES(?)", splice(^values)), select: e.c1

    assert TestRepo.to_sql(:all, query) ==
             {~s[SELECT f0."c1" FROM VALUES({$0:String},{$1:String},{$2:String},{$3:String}) AS f0],
              ["one", "two", "three", "four"]}

    assert TestRepo.to_inline_sql(:all, query) ==
             ~s[SELECT f0."c1" FROM VALUES('one','two','three','four') AS f0]

    assert TestRepo.all(query) == ["one", "two", "three", "four"]

    query =
      from e in fragment("VALUES(?)", splice(^values)),
        select: %{hash: fragment("cityHash64(?)", e.c1)}

    assert TestRepo.to_sql(:all, query) ==
             {~s[SELECT cityHash64(f0."c1") FROM VALUES({$0:String},{$1:String},{$2:String},{$3:String}) AS f0],
              ["one", "two", "three", "four"]}

    assert TestRepo.to_inline_sql(:all, query) ==
             ~s[SELECT cityHash64(f0."c1") FROM VALUES('one','two','three','four') AS f0]

    assert TestRepo.all(query) == [
             %{hash: 16_212_992_072_136_093_800},
             %{hash: 5_348_202_328_944_549_471},
             %{hash: 17_248_250_086_122_358_882},
             %{hash: 7_245_286_546_689_776_503}
           ]
  end
end
