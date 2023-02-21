defmodule Ecto.Integration.Case do
  use ExUnit.CaseTemplate
  alias Ecto.Integration.TestRepo

  setup do
    on_exit(fn ->
      %{rows: rows} = TestRepo.query!("show tables")

      # this includes schema_versions as well, but we don't
      # care since the database is recreated each time anew
      tables = Enum.map(rows, fn [table] -> table end)

      for table <- tables do
        TestRepo.query!("truncate #{table}", [], alter_sync: 1)
      end
    end)
  end
end
