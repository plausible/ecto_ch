defmodule Ecto.Integration.DateTime64Test do
  use Ecto.Integration.Case, async: true
  import Ecto.Query
  alias Ecto.Integration.TestRepo

  test "millisecond precision" do
    TestRepo.query!(
      "CREATE TABLE datetime64_3_test(i UInt8, d DateTime(3,'Etc/UTC')) ENGINE = Memory"
    )

    on_exit(fn -> TestRepo.query!("DROP TABLE datetime64_3_test") end)

    # https://clickhouse.com/docs/en/sql-reference/data-types/datetime64#examples
    TestRepo.query!(
      "INSERT INTO datetime64_3_test VALUES (1, 1546300800123), (2, 1546300800.123), (3, '2019-01-01 00:00:00')"
    )

    assert TestRepo.all(from t in "datetime64_3_test", select: map(t, [:i, :d])) == [
             %{i: 1, d: ~U[2019-01-01 00:00:00.123Z]},
             %{i: 2, d: ~U[2019-01-01 00:00:00.123Z]},
             %{i: 3, d: ~U[2019-01-01 00:00:00.000Z]}
           ]

    assert TestRepo.all(
             from t in "datetime64_3_test",
               where: t.d == ^~U[2019-01-01 00:00:00.123Z],
               select: t.i
           ) == [1, 2]

    assert TestRepo.all(
             from t in "datetime64_3_test",
               where: t.d > ^~U[2019-01-01 00:00:00.000Z],
               select: t.i
           ) == [1, 2]
  end

  # https://github.com/plausible/ecto_ch/issues/178
  test "microsecond precision" do
    TestRepo.query!(
      "CREATE TABLE datetime64_3_test(i UInt8, d DateTime(6,'Etc/UTC')) ENGINE = Memory"
    )

    on_exit(fn -> TestRepo.query!("DROP TABLE datetime64_3_test") end)

    TestRepo.query!(
      "INSERT INTO datetime64_3_test VALUES (1, 1546300800123456), (2, 1546300800.123456), (3, '2019-01-01 00:00:00')"
    )

    assert TestRepo.all(from t in "datetime64_3_test", select: map(t, [:i, :d])) == [
             %{i: 1, d: ~U[2019-01-01 00:00:00.123456Z]},
             %{i: 2, d: ~U[2019-01-01 00:00:00.123456Z]},
             %{i: 3, d: ~U[2019-01-01 00:00:00.000000Z]}
           ]

    assert TestRepo.all(
             from t in "datetime64_3_test",
               where: t.d == ^~U[2019-01-01 00:00:00.123456Z],
               select: t.i
           ) == [1, 2]

    assert TestRepo.all(
             from t in "datetime64_3_test",
               where: t.d > ^~U[2019-01-01 00:00:00.000Z],
               select: t.i
           ) == [1, 2]
  end
end
