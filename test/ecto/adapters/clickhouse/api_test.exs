defmodule Ecto.Adapters.ClickHouse.APITest do
  use ExUnit.Case, async: true

  alias Ecto.Adapters.ClickHouse
  alias Ecto.Adapters.ClickHouse.{Connection, API}

  import Ecto.Query

  defmodule InputSchema do
    use Ecto.Schema

    @primary_key false
    schema "input" do
      field :a, Ch, type: "Int16"
      field :b, :string
    end
  end

  defp plan(query, operation) do
    {query, _cast_params, dump_params} =
      Ecto.Adapter.Queryable.plan_query(operation, ClickHouse, query)

    {query, dump_params}
  end

  defp all(query) do
    {query, params} = plan(query, :all)

    query
    |> Connection.all(params)
    |> IO.iodata_to_binary()
  end

  describe "input/1" do
    test "schemaless" do
      query =
        from i in API.input(a: "Int16", b: :string),
          select: %{a: i.a, b: fragment("arrayReduce('argMaxState', [?], [?])", i.b, i.a)}

      assert all(query) == """
             SELECT f0."a",arrayReduce('argMaxState', [f0."b"], [f0."a"]) FROM input("a Int16, b String") AS f0\
             """
    end

    test "schemaful" do
      query =
        from i in API.input(InputSchema),
          select: %{a: i.a, b: fragment("arrayReduce('argMaxState', [?], [?])", i.b, i.a)}

      assert all(query) == """
             SELECT f0."a",arrayReduce('argMaxState', [f0."b"], [f0."a"]) FROM input("a Int16, b String") AS f0\
             """
    end
  end
end
