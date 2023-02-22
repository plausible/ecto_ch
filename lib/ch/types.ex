basic_types = [
  {:u8, UInt8, :integer},
  {:u16, UInt16, :integer},
  {:u32, UInt32, :integer},
  {:u64, UInt64, :integer},
  {:u128, UInt128, :integer},
  {:u256, UInt256, :integer},
  {:i8, Int8, :integer},
  {:i16, Int16, :integer},
  {:i32, Int32, :integer},
  {:i64, Int64, :integer},
  {:i128, Int128, :integer},
  {:i256, Int256, :integer},
  {:f32, Float32, :float},
  {:f64, Float64, :float}
]

for {type, name, base} <- basic_types do
  defmodule Module.concat(Ch.Types, name) do
    use Ecto.Type

    @impl true
    def type, do: unquote(type)

    @impl true
    def cast(value), do: Ecto.Type.cast(unquote(base), value)

    @impl true
    def dump(value), do: Ecto.Type.dump(unquote(base), value)

    @impl true
    def load(value), do: Ecto.Type.load(unquote(base), value)
  end
end

defmodule Ch.Types.FixedString do
  use Ecto.ParameterizedType

  @impl true
  def type(size), do: {:parameterized, :string, size}

  @impl true
  def init(opts) do
    size = Keyword.fetch!(opts, :size)
    (is_integer(size) and size > 0) || raise ":size needs to be a positive integer"
    size
  end

  @impl true
  def cast(value, _size), do: Ecto.Type.cast(:string, value)

  @impl true
  def dump(value, _dumper, _size), do: Ecto.Type.dump(:string, value)

  @impl true
  def load(value, _loader, _size), do: Ecto.Type.load(:string, value)
end

defmodule Ch.Types.Nullable do
  use Ecto.ParameterizedType

  @impl true
  def type(type), do: {:parameterized, :nullable, type}

  @impl true
  def init(opts) do
    type = Keyword.fetch!(opts, :type)
    is_atom(type) || raise ArgumentError, ":type needs to be an atom"

    try do
      type.type()
    rescue
      _ -> type
    end
  end

  @impl true
  def cast(value, type), do: Ecto.Type.cast(type, value)

  @impl true
  def dump(value, _dumper, type), do: Ecto.Type.dump(type, value)

  @impl true
  def load(value, _loader, type), do: Ecto.Type.load(type, value)
end

defmodule Ch.Types.Decimal do
  @moduledoc """
  Ecto type for for [`Decimal(P, S)`](https://clickhouse.com/docs/en/sql-reference/data-types/decimal/)
  """
  use Ecto.ParameterizedType

  @impl true
  def type({precision, scale}), do: {:parameterized, :decimal, {precision, scale}}

  @impl true
  def init(opts) do
    precision = Keyword.fetch!(opts, :precision)
    scale = Keyword.fetch!(opts, :scale)

    (is_integer(precision) and precision > 0) ||
      raise ArgumentError, ":precision needs to be a positive integer"

    (is_integer(scale) and scale >= 0) ||
      raise ArgumentError, ":scale needs to be a non-negative integer"

    {precision, scale}
  end

  @impl true
  def cast(value, _size), do: Ecto.Type.cast(:decimal, value)

  @impl true
  def dump(value, _dumper, _size), do: Ecto.Type.dump(:decimal, value)

  @impl true
  def load(value, _loader, _size), do: Ecto.Type.load(:decimal, value)
end
