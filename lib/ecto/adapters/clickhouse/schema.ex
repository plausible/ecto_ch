defmodule Ecto.Adapters.ClickHouse.Schema do
  @moduledoc false
  @conn Ecto.Adapters.ClickHouse.Connection
  @dialyzer :no_improper_lists

  def insert_all(
        adapter_meta,
        schema_meta,
        header,
        rows,
        on_conflict,
        returning,
        placeholders,
        opts
      ) do
    %{source: source, prefix: prefix, schema: schema} = schema_meta
    opts = [{:command, :insert} | opts]

    %{num_rows: num_rows} =
      case rows do
        {%Ecto.Query{} = _query, params} ->
          sql = @conn.insert(prefix, source, header, rows, on_conflict, returning, placeholders)
          Ecto.Adapters.SQL.query!(adapter_meta, sql, params, opts)

        rows when is_list(rows) ->
          types = prepare_types(schema, header, opts)
          opts = [{:types, types} | opts]
          # TODO use RowBinaryWithNamesAndTypes for type discrepancy warnings
          sql = [@conn.insert(prefix, source, header, []) | " FORMAT RowBinary"]
          rows = unzip_rows(rows, header)
          Ecto.Adapters.SQL.query!(adapter_meta, sql, rows, opts)
      end

    {num_rows, nil}
  end

  def insert(adapter_meta, schema_meta, params, opts) do
    %{source: source, prefix: prefix, schema: schema} = schema_meta
    {header, row} = :lists.unzip(params)

    types = prepare_types(schema, header, opts)
    sql = [@conn.insert(prefix, source, header, []) | " FORMAT RowBinary"]
    opts = [{:command, :insert}, {:types, types} | opts]

    Ecto.Adapters.SQL.query!(adapter_meta, sql, [row], opts)
    {:ok, []}
  end

  def delete(adapter_meta, schema_meta, params, opts) do
    %{source: source, prefix: prefix} = schema_meta
    filter_values = Keyword.values(params)
    sql = @conn.delete(prefix, source, params, [])
    Ecto.Adapters.SQL.query!(adapter_meta, sql, filter_values, opts)
    {:ok, []}
  end

  defp extract_types(schema, fields) do
    Enum.map(fields, fn field ->
      type = schema.__schema__(:type, field) || raise "missing type for " <> inspect(field)
      type |> Ecto.Type.type() |> remap_type()
    end)
  end

  defp prepare_types(schema, header, opts) do
    cond do
      schema ->
        extract_types(schema, header)

      types = opts[:types] ->
        Enum.map(header, fn field -> Access.fetch!(types, field) end)

      true ->
        raise ArgumentError, "missing :types"
    end
  end

  defp remap_type(dt) when dt in [:naive_datetime, :utc_datetime], do: :datetime

  defp remap_type(usec) when usec in [:naive_datetime_usec, :utc_datetime_usec] do
    {:datetime64, :microsecond}
  end

  # TODO :integer is used in schema_versions schema
  defp remap_type(:integer), do: :i64
  defp remap_type(b) when b in [:binary_id, :binary], do: :string

  # Ch.Types.FixedString, Ch.Types.Nullable, etc.
  defp remap_type({:parameterized, :ch, type}), do: type
  defp remap_type({:parameterized, Ch, type}), do: type
  defp remap_type({:array, type}), do: {:array, remap_type(type)}

  defp remap_type(other), do: other

  defp unzip_rows([row | rows], header) do
    [unzip_row(header, row) | unzip_rows(rows, header)]
  end

  defp unzip_rows([], _header), do: []

  defp unzip_row([field | fields], row) do
    case List.keyfind(row, field, 0) do
      {_, value} -> [value | unzip_row(fields, row)]
      nil = not_found -> [not_found | unzip_row(fields, row)]
    end
  end

  defp unzip_row([], _row), do: []
end
