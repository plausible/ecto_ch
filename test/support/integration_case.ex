defmodule Ecto.Integration.Case do
  use ExUnit.CaseTemplate
  alias Ecto.Integration.TestRepo

  setup do
    on_exit(fn ->
      # workaround for SHOW TABLES in clickhouse-local
      %{rows: rows} =
        TestRepo.query!("select name from system.tables where database = 'ecto_ch_test'")

      # this includes schema_migrations as well, but we don't
      # care since the database is recreated each time anew
      tables = Enum.map(rows, fn [table] -> table end)

      for table <- tables do
        TestRepo.query!("truncate ecto_ch_test.#{table}")
      end
    end)
  end
end
