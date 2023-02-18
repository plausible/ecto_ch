defmodule Ecto.Adapters.ClickHouseTest do
  use ExUnit.Case

  alias Ecto.Adapters.ClickHouse

  describe "storage_up/1" do
    test "create database" do
      opts = [database: "chto_temp_db"]

      assert :ok = ClickHouse.storage_up(opts)
      on_exit(fn -> ClickHouse.storage_down(opts) end)

      conn = start_supervised!(Ch)
      assert {:ok, %{rows: rows}} = Ch.query(conn, "show databases")
      assert ["chto_temp_db"] in rows
    end

    test "does not fail on second call" do
      opts = [database: "chto_temp_db_2"]

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

  describe "storage_down/2" do
    test "storage down (twice)" do
      opts = [database: "chto_temp_down_2"]

      assert :ok = ClickHouse.storage_up(opts)
      assert :ok = ClickHouse.storage_down(opts)

      conn = start_supervised!(Ch)
      assert {:ok, %{rows: rows}} = Ch.query(conn, "show databases")
      refute ["chto_temp_down_2"] in rows

      assert {:error, :already_down} = ClickHouse.storage_down(opts)
    end
  end
end
