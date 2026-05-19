defmodule Mix.Tasks.Ecto.Ch.Schema do
  @moduledoc """
  Shows an Ecto schema hint for a ClickHouse table.

  Examples:

      $ mix ecto.ch.schema
      $ mix ecto.ch.schema system.numbers
      $ mix ecto.ch.schema system.numbers --repo MyApp.Repo
  """
  use Mix.Task

  @conn Ecto.Adapters.ClickHouse.Connection

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

    repos = Mix.Ecto.parse_repo(kvs)

    config =
      Enum.find_value(repos, fn repo ->
        Mix.Ecto.ensure_repo(repo, kvs)

        if repo.__adapter__() == Ecto.Adapters.ClickHouse do
          repo.config()
        end
      end)

    {where, params} =
      case source do
        [nil, table] ->
          if config do
            database = Keyword.fetch!(config, :database)

            {"where database = {database:String} and table = {table:String}",
             %{"database" => database, "table" => table}}
          else
            {"where table = {table:String}", %{"table" => table}}
          end

        [database, table] ->
          {"where database = {database:String} and table = {table:String}",
           %{"database" => database, "table" => table}}
      end

    statement = "select database, name, type from system.columns " <> where

    case query(config || [], statement, params) do
      %{rows: [_ | _] = rows} ->
        ensure_single_table!(rows)

        schema = [
          """
          @primary_key false
          schema "#{table}" do
          """,
          Enum.map(rows, fn [_db, name, type] -> ["  ", build_field(name, type), ?\n] end),
          "end"
        ]

        IO.puts(schema)

      %{rows: []} ->
        raise "table not found"
    end
  end

  defp query(config, statement, params) do
    case Ch.start_link(@conn.start_options(config)) do
      {:ok, conn} ->
        try do
          case @conn.query(conn, statement, params, @conn.config_options(config)) do
            {:ok, result} -> result
            {:error, reason} -> raise reason
          end
        after
          Ch.stop(conn)
        end

      {:error, reason} ->
        raise reason
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

  defp ensure_single_table!(rows) do
    rows
    |> Enum.group_by(fn [db, _, _] -> db end, fn [_, name, type] -> [name, type] end)
    |> Map.keys()
    |> case do
      [_db] ->
        :ok

      dbs ->
        raise """
        table is present in multiple databases: #{Enum.join(dbs, ", ")}
        """
    end
  end
end
