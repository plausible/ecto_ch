defmodule Ecto.Adapters.ClickHouse.Schema do
  @moduledoc false
  @conn Ecto.Adapters.ClickHouse.Connection

  # def insert_stream(schema_or_source, stream, {adapter_meta, opts}) do
  #   {sql, opts} = build_insert(schema_or_source, opts)
  #   opts = [{:command, :insert} | opts]
  #   %{num_rows: num_rows} = Ecto.Adapters.SQL.query!(adapter_meta, sql, stream, opts)
  #   return = if opts[:returning], do: []
  #   {num_rows, return}
  # end

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
          types =
            if schema do
              extract_types(schema, header)
            else
              opts[:types] || raise "missing :types"
            end

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
    {header, row} = Enum.unzip(params)

    types =
      if schema do
        extract_types(schema, header)
      else
        opts[:types] || raise "missing :types"
      end

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

  # TODO
  defp remap_type(:naive_datetime), do: :datetime
  defp remap_type(:integer), do: :i64
  defp remap_type(other), do: other

  # defp build_insert(source, opts) when is_binary(source) do
  #   prefix = Keyword.get(opts, :prefix)
  #   fields = Keyword.get(opts, :fields)
  #   sql = @conn.insert(prefix, source, fields, [])
  #   {sql, opts}
  # end

  # defp build_insert(schema, opts) when is_atom(schema) do
  #   prefix = schema.__schema__(:prefix)
  #   table = schema.__schema__(:source)
  #   fields = schema.__schema__(:fields)
  #   types = extract_types(schema, fields)

  #   sql = @conn.insert(prefix, table, fields, [])
  #   opts = [{:types, types} | opts]

  #   {sql, opts}
  # end

  # defp build_insert({source, schema}, opts) when is_atom(schema) do
  #   prefix = schema.__schema__(:prefix)
  #   fields = schema.__schema__(:fields)
  #   types = extract_types(schema, fields)

  #   sql = @conn.insert(prefix, source, fields, [])
  #   opts = [{:types, types} | opts]

  #   {sql, opts}
  # end

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
