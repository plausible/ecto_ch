defmodule Mix.Tasks.Ecto.Ch.Schema do
  @moduledoc """
  Shows an Ecto schema hint for a ClickHouse table.

  Examples:

      $ mix ecto.ch.schema
      $ mix ecto.ch.schema system.numbers
      $ mix ecto.ch.schema system.numbers --repo MyApp.Repo
  """
  use Mix.Task
  alias Ch.Connection, as: Conn

  def run([]) do
    IO.puts(@moduledoc)
  end

  def run(["-" <> _k | _] = kvs) do
    run(_source = nil, kvs)
  end

  def run([source | kvs]) do
    run(source, kvs)
  end

  defp run(source, kvs) do
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

    repos = Mix.Ecto.parse_repo(kvs)

    config =
      Enum.find_value(repos, fn repo ->
        Mix.Ecto.ensure_repo(repo, kvs)
        repo.__adapter__() == Ecto.Adapters.ClickHouse
        repo.config()
      end)

    conn = connect(config || [])

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
    name = Code.format_string!(":#{name}")

    case {ecto_type, clickhouse_type} do
      {ecto_type, nil} ->
        ~s[field #{name}, #{inspect(ecto_type)}]

      {ecto_type, clickhouse_type} ->
        ~s[field #{name}, #{inspect(ecto_type)}, type: "#{clickhouse_type}"]
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
