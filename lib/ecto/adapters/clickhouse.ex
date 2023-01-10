defmodule Ecto.Adapters.ClickHouse do
  use Ecto.Adapters.SQL, driver: :ch

  @impl Ecto.Adapter.Migration
  def supports_ddl_transaction?, do: false

  @impl Ecto.Adapter.Migration
  def lock_for_migrations(_meta, _options, f), do: f.()

  @impl Ecto.Adapter.Schema
  def autogenerate(:id), do: nil
  def autogenerate(:embed_id), do: Ecto.UUID.generate()
  def autogenerate(:binary_id), do: Ecto.UUID.generate()

  @impl Ecto.Adapter.Queryable
  def prepare(_operation, query), do: {:nocache, query}

  @impl Ecto.Adapter.Queryable
  def execute(adapter_meta, query_meta, {:nocache, query} = q, params, opts) do
    q = put_elem(q, 1, {_id = 0, @conn.all(query, params)})
    Ecto.Adapters.SQL.execute(:unnamed, adapter_meta, query_meta, q, params, opts)
  end

  @impl Ecto.Adapter
  defmacro __before_compile__(env) do
    [
      Ecto.Adapters.SQL.__before_compile__(@driver, env),
      quote do
        def insert_stream(table_or_schema, rows, opts \\ []) do
          Ecto.Adapters.ClickHouse.insert_stream(get_dynamic_repo(), table_or_schema, rows, opts)
        end
      end
    ]
  end

  def insert_stream(repo, table, rows, opts) do
    {statement, opts} = build_insert(table, opts)
    opts = put_in(opts, [:command], :insert)

    with {:ok, %{num_rows: num_rows}} <- Ecto.Adapters.SQL.query(repo, statement, rows, opts) do
      {:ok, num_rows}
    end
  end

  defp build_insert(table, opts) when is_binary(table) do
    statement = build_insert_statement(opts[:prefix], table, opts[:fields])
    {statement, opts}
  end

  defp build_insert(schema, opts) when is_atom(schema) do
    prefix = schema.__schema__(:prefix)
    table = schema.__schema__(:source)
    fields = schema.__schema__(:fields)

    types =
      Enum.map(fields, fn field ->
        :type |> schema.__schema__(field) |> Ecto.Type.type() |> remap_type()
      end)

    statement = build_insert_statement(prefix, table, fields)
    opts = put_in(opts, [:types], types)
    {statement, opts}
  end

  # used in benchmark
  @doc false
  def build_insert_statement(prefix, table, fields) do
    ["INSERT INTO ", quote_table(prefix, table) | encode_fields(fields)]
  end

  defp encode_fields(fields) do
    case fields do
      [_ | _] = fields -> [?(, intersperce_map(fields, ?,, &quote_name/1), ?)]
      _none -> []
    end
  end

  defp quote_table(prefix, name)
  defp quote_table(nil, name), do: quote_name(name)
  defp quote_table(prefix, name), do: [quote_name(prefix), ?., quote_name(name)]

  # TODO
  defp remap_type(:naive_datetime), do: :datetime
  defp remap_type(other), do: other

  defp intersperce_map([elem], _separator, mapper), do: [mapper.(elem)]

  defp intersperce_map([elem | rest], separator, mapper) do
    [mapper.(elem), separator | intersperce_map(rest, separator, mapper)]
  end

  defp intersperce_map([], _separator, _mapper), do: []

  defp quote_name(name, quoter \\ ?")
  defp quote_name(nil, _), do: []

  defp quote_name(name, quoter) when is_atom(name) do
    name |> Atom.to_string() |> quote_name(quoter)
  end

  defp quote_name(name, quoter) do
    if String.contains?(name, <<quoter>>) do
      raise "bad name #{inspect(name)}"
    end

    [quoter, name, quoter]
  end
end
