defmodule Ecto.Adapters.ClickHouse.Storage do
  @moduledoc false
  alias Ch.{Query, Error}
  alias Ch.Connection, as: Conn

  @conn Ecto.Adapters.ClickHouse.Connection

  def storage_up(opts) do
    {database, opts} = Keyword.pop!(opts, :database)
    statement = "CREATE DATABASE #{@conn.quote_name(database)}"

    with {:ok, conn} <- Conn.connect(opts),
         {:ok, _result, _conn} <- exec(conn, statement),
         do: :ok
  end

  def storage_down(opts) do
    {database, opts} = Keyword.pop!(opts, :database)
    statement = "DROP DATABASE #{@conn.quote_name(database)}"

    with {:ok, conn} <- Conn.connect(opts),
         {:ok, _result, _conn} <- exec(conn, statement),
         do: :ok
  end

  def storage_status(opts) do
    {database, opts} = Keyword.pop!(opts, :database)
    statement = "SELECT 1 FROM system.databases WHERE name = {database:String}"
    params = %{"database" => database}

    with {:ok, conn} <- Conn.connect(opts),
         {:ok, %{num_rows: num_rows}, _conn} <- exec(conn, statement, params) do
      case num_rows do
        1 -> :up
        0 -> :down
      end
    end
  end

  defp exec(conn, sql, params \\ [], opts \\ []) do
    query = Query.build(sql, command: opts[:command])

    case Conn.handle_execute(query, params, [], conn) do
      {:ok, _query, result, conn} -> {:ok, result, conn}
      {:disconnect, reason, _conn} -> {:error, reason}
      {:error, %Error{code: 82}, _conn} -> {:error, :already_up}
      {:error, %Error{code: 81}, _conn} -> {:error, :already_down}
      {:error, reason, _conn} -> {:error, reason}
    end
  end
end
