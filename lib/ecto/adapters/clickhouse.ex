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

  def insert_stream(repo, table_or_schema, rows, opts) do
    {statement, opts} = build_insert(table_or_schema, opts)
    types = opts[:types] || raise "missing :types for insert"
    rows = Stream.map(rows, fn row -> Ch.Protocol.encode_row(row, types) end)

    with {:ok, %{num_rows: num_rows}} <-
           Ecto.Adapters.SQL.query(repo, statement, rows, opts) do
      {:ok, num_rows}
    end
  end

  # used for benchmarks
  @doc false
  def build_insert(table, opts) do
    fields =
      case opts[:fields] do
        [_ | _] = fields -> [?(, intersperce_map(fields, ?,, &quote_name/1), ?)]
        _none -> []
      end

    statement = ["INSERT INTO ", quote_name(table), fields | " FORMAT RowBinary"]
    opts = put_in(opts, [:command], :insert)
    {statement, opts}
  end

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
