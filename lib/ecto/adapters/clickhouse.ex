defmodule Ecto.Adapters.ClickHouse do
  @moduledoc "TODO"
  # TODO fix warnings
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
  def execute(adapter_meta, query_meta, {:nocache, query} = prepared, params, opts) do
    {sql, params} = @conn.all(query, params)
    prepared = put_elem(prepared, 1, {_id = 0, sql})
    Ecto.Adapters.SQL.execute(:unnamed, adapter_meta, query_meta, prepared, params, opts)
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

  @impl Ecto.Adapter.Schema
  def insert_all(
        adapter_meta,
        schema_meta,
        header,
        rows,
        _on_conflict,
        _returning,
        _placeholders,
        opts
      ) do
    %{source: source, prefix: prefix, schema: schema} = schema_meta

    types =
      if schema do
        extract_types(schema, header)
      else
        opts[:types] || raise "missing :types"
      end

    # TODO support queries like INSERT INTO ... SELECT FROM
    rows = unzip_insert(rows, header)
    sql = @conn.insert(prefix, source, header)
    opts = [{:types, types}, {:command, :insert} | opts]

    %{num_rows: num, rows: rows} = Ecto.Adapters.SQL.query!(adapter_meta, sql, rows, opts)
    {num, rows}
  end

  # TODO support queries and placeholders, benchmark
  defp unzip_insert([row | rows], header) do
    [unzip_row(header, row) | unzip_insert(rows, header)]
  end

  defp unzip_insert([], _header), do: []

  defp unzip_row([field | fields], row) do
    case :lists.keyfind(field, 1, row) do
      {_, value} -> [value | unzip_row(fields, row)]
      false -> [nil | unzip_row(fields, row)]
    end
  end

  defp unzip_row([], _row), do: []

  # TODO
  @impl Ecto.Adapter.Schema
  def insert(_adapter_meta, _schema_meta, _params, _on_conflict, _returning, _opts) do
    raise "not implemented, please use insert_all/2 or insert_stream/2 instead"
  end

  @impl Ecto.Adapter.Schema
  def update(_adapter_meta, _schema_meta, _fields, _params, _returning, _opts) do
    raise "not implemented"
  end

  @impl Ecto.Adapter.Schema
  def delete(_adapter_meta, _schema_meta, _params, _opts) do
    raise "not implemented"
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
    types = extract_types(schema, fields)
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

  defp extract_types(schema, fields) do
    Enum.map(fields, fn field ->
      :type |> schema.__schema__(field) |> Ecto.Type.type() |> remap_type()
    end)
  end

  # TODO
  defp remap_type(:naive_datetime), do: :datetime
  defp remap_type(other), do: other
end
