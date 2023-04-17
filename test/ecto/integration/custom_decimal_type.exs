defmodule Ecto.Integration.CustomDecimalType do
  use Ecto.Integration.Case

  alias Ecto.Integration.TestRepo

  alias __MODULE__.DecimalCaseTable

  defmodule __MODULE__.Decimal18 do
    use Ecto.ParameterizedType

    @impl true
    def type(type), do: {:parameterized, :ch, type}

    @impl true
    def init(opts) do
      scale = Keyword.fetch!(opts, :scale)

      (is_integer(scale) and scale >= 0) ||
        raise ArgumentError, ":scale needs to be a non-negative integer"

      {:decimal, 18, scale}
    end

    @impl true
    def cast(value, _type), do: Ecto.Type.cast(:decimal, value)

    @impl true
    def dump(value, _dumper, _type), do: Ecto.Type.dump(:decimal, value)

    @impl true
    def load(value, _loader, _type), do: Ecto.Type.load(:decimal, value)
  end

  defmodule __MODULE__.DecimalCaseTable do
    alias Ecto.Integration.CustomDecimalType.Decimal18

    use Ecto.Schema

    @primary_key false
    schema "decimal_case_custom_type" do
      field(:dec_field, Decimal18, scale: 4)
    end
  end

  test "can encode custom Decimal18 type" do
    raw_sql = """
      CREATE TABLE if not exists decimal_case_custom_type (
        dec_field Decimal(18, 4)
      ) ENGINE = MergeTree
      ORDER BY (dec_field)
    """

    {:ok, _} = Ecto.Adapters.SQL.query(TestRepo, raw_sql, [])

    TestRepo.insert!(%DecimalCaseTable{dec_field: Decimal.new(5)})
  end
end
