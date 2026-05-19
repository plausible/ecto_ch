defmodule Ecto.Adapters.ClickHouse.Storage do
  @moduledoc false
  alias Ch.Error

  @conn Ecto.Adapters.ClickHouse.Connection

  def storage_up(opts) do
    {database, opts} = Keyword.pop!(opts, :database)
    statement = "CREATE DATABASE #{@conn.quote_name(database)}"

    with_pool(opts, fn conn, query_opts ->
      with {:ok, _result} <- exec(conn, statement, [], query_opts), do: :ok
    end)
  end

  def storage_down(opts) do
    {database, opts} = Keyword.pop!(opts, :database)
    statement = "DROP DATABASE #{@conn.quote_name(database)}"

    with_pool(opts, fn conn, query_opts ->
      with {:ok, _result} <- exec(conn, statement, [], query_opts), do: :ok
    end)
  end

  def storage_status(opts) do
    {database, opts} = Keyword.pop!(opts, :database)
    statement = "SELECT 1 FROM system.databases WHERE name = {database:String}"
    params = %{"database" => database}

    with_pool(opts, fn conn, query_opts ->
      with {:ok, %{num_rows: num_rows}} <- exec(conn, statement, params, query_opts) do
        case num_rows do
          1 -> :up
          0 -> :down
        end
      end
    end)
  end

  defp with_pool(opts, fun) do
    case Ch.start_link(@conn.start_options(opts)) do
      {:ok, conn} ->
        try do
          fun.(conn, @conn.config_options(opts))
        after
          Ch.stop(conn)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp exec(conn, sql, params, opts) do
    case @conn.query(conn, sql, params, opts) do
      {:ok, result} -> {:ok, result}
      {:error, %Error{code: 82}} -> {:error, :already_up}
      {:error, %Error{code: 81}} -> {:error, :already_down}
      {:error, reason} -> {:error, reason}
    end
  end
end
