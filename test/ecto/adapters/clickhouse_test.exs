defmodule Ecto.Adapters.ClickHouseTest do
  use ExUnit.Case

  alias Ecto.Adapters.ClickHouse
  alias Ecto.Integration.TestRepo
  import ExUnit.CaptureLog

  describe "storage_up/1" do
    test "create database" do
      opts = [database: "ecto_ch_temp_db"]

      assert :ok = ClickHouse.storage_up(opts)
      on_exit(fn -> ClickHouse.storage_down(opts) end)

      conn = start_supervised!(Ch)
      assert {:ok, %{rows: rows}} = Ch.query(conn, "show databases")
      assert ["ecto_ch_temp_db"] in rows
    end

    test "does not fail on second call" do
      opts = [database: "ecto_ch_temp_db_2"]

      assert :ok = ClickHouse.storage_up(opts)
      on_exit(fn -> ClickHouse.storage_down(opts) end)

      assert {:error, :already_up} = ClickHouse.storage_up(opts)
    end

    test "fails if no database is specified" do
      assert_raise KeyError, ~r"key :database not found in:", fn ->
        ClickHouse.storage_up([])
      end
    end
  end

  describe "storage_down/1" do
    test "storage down (twice)" do
      opts = [database: "ecto_ch_temp_down_2"]

      assert :ok = ClickHouse.storage_up(opts)
      assert :ok = ClickHouse.storage_down(opts)

      conn = start_supervised!(Ch)
      assert {:ok, %{rows: rows}} = Ch.query(conn, "show databases")
      refute ["ecto_ch_temp_down_2"] in rows

      assert {:error, :already_down} = ClickHouse.storage_down(opts)
    end
  end

  describe "storage_status/1" do
    test "when database is down" do
      opts = [database: "ecto_ch_temp_status_down"]
      assert ClickHouse.storage_status(opts) == :down
    end

    test "when database is up" do
      opts = [database: "ecto_ch_temp_status_up"]

      :ok = ClickHouse.storage_up(opts)
      on_exit(fn -> ClickHouse.storage_down(opts) end)

      assert ClickHouse.storage_status(opts) == :up
    end
  end

  describe "transaction callbacks" do
    test "in_transaction?/1 is false for an idle checked out connection" do
      TestRepo.checkout(fn ->
        meta = Ecto.Adapter.lookup_meta(TestRepo.get_dynamic_repo())
        refute ClickHouse.in_transaction?(meta)
      end)
    end

    test "transaction/3 uses the checked out transactional connection" do
      with_transaction_connection(fn meta ->
        assert {:ok, :inside_transaction} =
                 ClickHouse.transaction(meta, [], fn ->
                   assert ClickHouse.in_transaction?(meta)
                   :inside_transaction
                 end)
      end)
    end

    test "rollback/2 aborts the checked out transactional connection" do
      capture_log(fn ->
        with_transaction_connection(fn meta ->
          assert {:error, :rolled_back} =
                   ClickHouse.transaction(meta, [], fn ->
                     assert ClickHouse.in_transaction?(meta)
                     ClickHouse.rollback(meta, :rolled_back)
                   end)
        end)
      end)
    end

    test "rollback/2 raises outside a transaction" do
      TestRepo.checkout(fn ->
        meta = Ecto.Adapter.lookup_meta(TestRepo.get_dynamic_repo())

        assert_raise RuntimeError, "cannot call rollback outside of transaction", fn ->
          ClickHouse.rollback(meta, :rolled_back)
        end
      end)
    end
  end

  defp with_transaction_connection(fun) do
    TestRepo.checkout(fn ->
      meta = Ecto.Adapter.lookup_meta(TestRepo.get_dynamic_repo())
      key = sql_conn_key(meta.pid)
      conn = Process.get(key)
      Process.put(key, %{conn | conn_mode: :transaction})

      try do
        fun.(meta)
      after
        Process.put(key, conn)
      end
    end)
  end

  defp sql_conn_key(pool), do: {Ecto.Adapters.SQL, pool}
end
