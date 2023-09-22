defmodule Ecto.Adapters.ClickHouse.API do
  @moduledoc """
  Helpers for building Ecto queries for ClickHouse.
  """

  import Ecto.Query

  @doc """
  Builds an [`input(structure)`](https://clickhouse.com/docs/en/sql-reference/table-functions/input) that can be used in `Ecto.Query.from`

      from i in input(a: "Int16", b: :string)
      from i in input(Schema)

  """
  def input(schema) when is_list(schema) do
    structure =
      schema
      |> Enum.map(fn {name, type} ->
        type =
          case type do
            _ when is_binary(type) -> type
            _ -> Ch.Types.encode(type)
          end

        IO.iodata_to_binary([to_string(name), ?\s, type])
      end)
      |> Enum.join(", ")

    %Ecto.Query{from: %Ecto.Query.FromExpr{source: {:fragment, _ctx = [], fragment}} = from} =
      query = from(fragment("input(?)", literal(^structure)))

    # TODO or maybe build query struct manually with a custom :fragment (like a custom op)
    #      which would be handled in connection.ex
    %{query | from: %{from | source: {:fragment, [input: schema], fragment}}}
  end

  def input(schema) when is_atom(schema) do
    query =
      schema.__schema__(:fields)
      |> Enum.map(fn field ->
        type = schema.__schema__(:type, field)
        type || raise "missing type for field " <> inspect(field)
        {field, Ecto.Adapters.ClickHouse.Schema.remap_type(type, schema, field)}
      end)
      |> input()

    # TODO
    # %{query | sources: {nil, schema}}
    query
  end

  @doc """
  Adds [SAMPLE](https://clickhouse.com/docs/en/sql-reference/statements/select/sample) clause to the query.

      # sample k
      from s in sample(Schema, 0.1)
      from s in sample("table", 0.1)

      # sample n
      from s in sample(Schema, 10000)
      from s in sample("table", 10000)

  """
  def sample(source, sample) when is_number(sample) do
    %{from: from} = query = from(source)
    %{query | from: Map.put(from, :ecto_ch, %{sample: sample})}
  end

  @doc """
  Adds [FINAL](https://clickhouse.com/docs/en/sql-reference/statements/select/from#final-modifier) modifier to the query.

      from f in final(Schema)
      from f in final("table")

  """
  def final(source) do
    %{from: from} = query = from(source)
    %{query | from: Map.put(from, :ecto_ch, %{final: true})}
  end
end
