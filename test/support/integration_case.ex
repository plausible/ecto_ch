defmodule Ecto.Integration.Case do
  use ExUnit.CaseTemplate
  alias Ecto.Integration.TestRepo

  setup do
    on_exit(fn ->
      %{rows: rows} = TestRepo.query!("show tables")

      # this includes schema_migrations as well, but we don't
      # care since the database is recreated each time anew
      tables = Enum.map(rows, fn [table] -> table end)

      for table <- tables do
        TestRepo.query!("truncate #{table}")
      end
    end)
  end

  using do
    quote do
      import Ecto.Integration.Case
    end
  end

  # shifts naive datetimes for non-utc timezones into utc to match ClickHouse behaviour
  # see https://clickhouse.com/docs/en/sql-reference/data-types/datetime#usage-remarks
  def to_clickhouse_naive(%NaiveDateTime{} = naive_datetime) do
    case TestRepo.query!("select timezone()").rows do
      [["UTC"]] ->
        naive_datetime

      [[timezone]] ->
        naive_datetime
        |> DateTime.from_naive!(timezone)
        |> DateTime.shift_zone!("Etc/UTC")
        |> DateTime.to_naive()
    end
  end
end
