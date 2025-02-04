defmodule Ecto.Adapters.ClickHouseTest do
  use ExUnit.Case
  import Ecto.Integration.Case, only: [client_opts: 0, client_opts: 1]
  alias Ecto.Adapters.ClickHouse

  describe "storage_up/1" do
    test "create database" do
      opts = client_opts(database: "ecto_ch_temp_db")

      assert :ok = ClickHouse.storage_up(opts)
      on_exit(fn -> ClickHouse.storage_down(opts) end)

      conn = start_supervised!({Ch, client_opts()})
      assert {:ok, %{rows: rows}} = Ch.query(conn, "show databases")
      assert ["ecto_ch_temp_db"] in rows
    end

    test "does not fail on second call" do
      opts = client_opts(database: "ecto_ch_temp_db_2")

      assert :ok = ClickHouse.storage_up(opts)
      on_exit(fn -> ClickHouse.storage_down(opts) end)

      assert {:error, :already_up} = ClickHouse.storage_up(opts)
    end

    test "fails if no database is specified" do
      assert_raise KeyError, "key :database not found in: []", fn ->
        ClickHouse.storage_up([])
      end
    end
  end

  describe "storage_down/1" do
    test "storage down (twice)" do
      opts = client_opts(database: "ecto_ch_temp_down_2")

      assert :ok = ClickHouse.storage_up(opts)
      assert :ok = ClickHouse.storage_down(opts)

      conn = start_supervised!({Ch, client_opts()})
      assert {:ok, %{rows: rows}} = Ch.query(conn, "show databases")
      refute ["ecto_ch_temp_down_2"] in rows

      assert {:error, :already_down} = ClickHouse.storage_down(opts)
    end
  end

  describe "storage_status/1" do
    test "when database is down" do
      opts = client_opts(database: "ecto_ch_temp_status_down")
      assert ClickHouse.storage_status(opts) == :down
    end

    test "when database is up" do
      opts = client_opts(database: "ecto_ch_temp_status_up")

      :ok = ClickHouse.storage_up(opts)
      on_exit(fn -> ClickHouse.storage_down(opts) end)

      assert ClickHouse.storage_status(opts) == :up
    end
  end
end
