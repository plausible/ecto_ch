defmodule Ecto.Adapters.ClickHouse.Schema do
  @moduledoc false
  @conn Ecto.Adapters.ClickHouse.Connection

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

    %{num_rows: num_rows} =
      case rows do
        {%Ecto.Query{} = _query, _params} ->
          sql = @conn.insert(prefix, source, header, rows, on_conflict, returning, placeholders)
          opts = [{:command, :insert} | opts]
          Ecto.Adapters.SQL.query!(adapter_meta, sql, [], opts)

        rows when is_list(rows) ->
          types = prepare_types(schema, header, opts)

          rows =
            rows
            |> unzip_insert(header)
            # TODO
            |> Stream.chunk_every(50)
            |> Stream.map(fn chunk -> Ch.RowBinary.encode_rows(chunk, types) end)

          sql = @conn.insert(prefix, source, header, [])
          opts = [{:command, :insert}, {:format, "RowBinary"} | opts]
          Ecto.Adapters.SQL.query!(adapter_meta, sql, rows, opts)
      end

    {num_rows, nil}
  end

  def insert(adapter_meta, schema_meta, params, opts) do
    %{source: source, prefix: prefix, schema: schema} = schema_meta
    {header, row} = :lists.unzip(params)

    types = prepare_types(schema, header, opts)
    sql = @conn.insert(prefix, source, header, [])
    opts = [{:command, :insert}, {:format, "RowBinary"} | opts]
    row = Ch.RowBinary.encode_row(row, types)

    Ecto.Adapters.SQL.query!(adapter_meta, sql, [row], opts)
    {:ok, []}
  end

  def delete(adapter_meta, %{source: source, prefix: prefix}, params, opts) do
    filter_values = Keyword.values(params)
    sql = @conn.delete(prefix, source, params, [])
    Ecto.Adapters.SQL.query!(adapter_meta, sql, filter_values, opts)
    {:ok, []}
  end

  defp extract_types(schema, fields) do
    Enum.map(fields, fn field ->
      schema.__schema__(:type, field)
      |> Ecto.Type.type()
      |> remap_type()
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

  defp remap_type(:naive_datetime), do: :datetime
  defp remap_type(:utc_datetime), do: :datetime
  defp remap_type(:naive_datetime_usec), do: {:datetime64, :microsecond}
  defp remap_type(:utc_datetime_usec), do: {:datetime64, :microsecond}
  # TODO :integer is used in schema_versions schema
  defp remap_type(:integer), do: :i64
  defp remap_type(other), do: other

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
end
