defmodule Ecto.Adapters.ClickHouse.Schema do
  @moduledoc false
  @conn Ecto.Adapters.ClickHouse.Connection
  @dialyzer :no_improper_lists

  alias Ch.RowBinary

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
          # TODO use RowBinaryWithNamesAndTypes for type discrepancy warnings
          sql = [@conn.insert(prefix, source, header, []) | " FORMAT RowBinary"]
          data = unzip_encode_rows(rows, header, types)
          Ecto.Adapters.SQL.query!(adapter_meta, sql, {:raw, data}, opts)
      end

    {num_rows, nil}
  end

  def insert(adapter_meta, schema_meta, params, opts) do
    %{source: source, prefix: prefix, schema: schema} = schema_meta
    {header, row} = :lists.unzip(params)

    types = prepare_types(schema, header, opts)
    sql = [@conn.insert(prefix, source, header, []) | " FORMAT RowBinary"]
    data = RowBinary.encode_row(row, types)
    opts = [{:command, :insert} | opts]

    Ecto.Adapters.SQL.query!(adapter_meta, sql, {:raw, data}, opts)
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
  defp remap_type({:array, type}), do: {:array, remap_type(type)}

  defp remap_type(other), do: other

  defp unzip_encode_rows([row | rows], header, types) do
    [unzip_encode_row(header, types, row) | unzip_encode_rows(rows, header, types)]
  end

  defp unzip_encode_rows([], _header, _types), do: []

  defp unzip_encode_row([field | fields], [type | types], row) do
    case :lists.keyfind(field, 1, row) do
      {_, value} -> [RowBinary.encode(type, value) | unzip_encode_row(fields, types, row)]
      false -> [RowBinary.encode(type, nil) | unzip_encode_row(fields, types, row)]
    end
  end

  defp unzip_encode_row([], [], _row), do: []
end
