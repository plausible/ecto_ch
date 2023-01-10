defmodule Ecto.Adapters.ClickHouse do
  use Ecto.Adapters.SQL, driver: :ch
  @conn __MODULE__.Connection

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

  @doc false
  def build_insert(table, opts) when is_binary(table) do
    statement = build_insert_statement(opts[:prefix], table, opts[:fields])
    {statement, opts}
  end

  def build_insert(schema, opts) when is_atom(schema) do
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

  defp build_insert_statement(prefix, table, fields) do
    fields =
      case fields do
        [_ | _] = fields -> [?(, @conn.intersperce_map(fields, ?,, &@conn.quote_name/1), ?)]
        _none -> []
      end

    ["INSERT INTO ", @conn.quote_table(prefix, table) | fields]
  end

  # TODO
  defp remap_type(:naive_datetime), do: :datetime
  defp remap_type(other), do: other
end
