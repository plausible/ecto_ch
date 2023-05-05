defmodule Mix.Tasks.Ecto.Ch.Schema do
  @moduledoc """
  Shows an Ecto schema hint for a table.

  Examples:

      $ mix ecto.ch.schema
      $ mix ecto.ch.schema system.numbers
  """
  use Mix.Task
  alias Ch.Connection, as: Conn

  def run([]) do
    IO.puts(@moduledoc)
  end

  def run([source]) do
    [_, table] =
      source =
      case String.split(source, ".") do
        [_table] = source -> [nil | source]
        [_prefix, _table] = source -> source
      end

    {where, params} =
      case source do
        [nil, table] ->
          {"where table = {table:String}", %{"table" => table}}

        [database, table] ->
          {"where database = {database:String} and table = {table:String}",
           %{"database" => database, "table" => table}}
      end

    statement = "select name, type from system.columns " <> where

    conn = connect(_config = [])

    case query(conn, statement, params) do
      {%Ch.Result{rows: [_ | _] = rows}, _conn} ->
        schema = [
          """
          @primary_key false
          schema "#{table}" do
          """,
          Enum.map(rows, fn [name, type] -> ["  ", build_field(name, type), ?\n] end),
          "end"
        ]

        IO.puts(schema)

      {%Ch.Result{rows: []}, _conn} ->
        raise "table not found"
    end
  end

  defp connect(config) do
    case Conn.connect(config) do
      {:ok, conn} -> conn
      {:error, reason} -> raise reason
    end
  end

  defp query(conn, statement, params, opts \\ []) do
    query = Ch.Query.build(statement)
    params = DBConnection.Query.encode(query, params, opts)

    case Conn.handle_execute(query, params, opts, conn) do
      {:ok, query, result, conn} -> {DBConnection.Query.decode(query, result, opts), conn}
      {:disconnect, reason, _conn} -> raise reason
      {:error, reason, _conn} -> raise reason
    end
  end

  @doc false
  def build_field(name, type) do
    type = Ch.Types.decode(type)

    ecto_type = ecto_type(type)
    clickhouse_type = clickhouse_type(type)

    case {ecto_type, clickhouse_type} do
      {ecto_type, nil} ->
        ~s[field :"#{name}", #{inspect(ecto_type)}]

      {ecto_type, clickhouse_type} ->
        ~s[field :"#{name}", #{inspect(ecto_type)}, type: "#{clickhouse_type}"]
    end
  end

  defp ecto_type({:array, type}), do: {:array, ecto_type(type)}
  defp ecto_type(type) when type in [:string, :date, :boolean], do: type
  defp ecto_type(:uuid), do: Ecto.UUID
  defp ecto_type(_type), do: Ch

  defp clickhouse_type({:array, type}), do: clickhouse_type(type)
  defp clickhouse_type(type) when type in [:uuid, :string, :date, :boolean], do: nil
  defp clickhouse_type(type), do: Ch.Types.encode(type)
end
