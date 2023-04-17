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
  def type(type), do: {:parameterized, :ch, type}

  @impl true
  def init(opts) do
    size = Keyword.fetch!(opts, :size)
    (is_integer(size) and size > 0) || raise ":size needs to be a positive integer"
    {:string, size}
  end

  @impl true
  def cast(value, _type), do: Ecto.Type.cast(:string, value)

  @impl true
  def dump(value, _dumper, _type), do: Ecto.Type.dump(:string, value)

  @impl true
  def load(value, _loader, _type), do: Ecto.Type.load(:string, value)
end

defmodule Ch.Types.Nullable do
  use Ecto.ParameterizedType
  @dialyzer :no_improper_lists

  @impl true
  def type(state), do: {:parameterized, :ch, ch_type(state)}

  @impl true
  def init(opts) do
    ecto_type = Keyword.fetch!(opts, :type)

    is_atom(ecto_type) ||
      raise ArgumentError,
            """
            :type needs to be one of:
            - an Ecto.Type like Ecto.UUID
            - an atom like :string
            - a tuple representing a composite type like {:array, type}
            """

    ch_type =
      try do
        ecto_type.type()
      rescue
        _ -> ecto_type
      end

    state(ecto_type, {:nullable, ch_type})
  end

  @impl true
  def cast(value, state), do: Ecto.Type.cast(ecto_type(state), value)

  @impl true
  def dump(value, _dumper, state), do: Ecto.Type.dump(ecto_type(state), value)

  @impl true
  def load(value, _loader, state), do: Ecto.Type.load(ecto_type(state), value)

  @compile inline: [state: 2, ecto_type: 1, ch_type: 1]
  defp state(ecto_type, ch_type), do: [ecto_type | ch_type]
  defp ecto_type([t | _]), do: t
  defp ch_type([_ | t]), do: t
end

for size <- [32, 64, 128, 256] do
  defmodule Module.concat(Ch.Types, :"Decimal#{size}") do
    use Ecto.ParameterizedType

    @impl true
    def type(type), do: {:parameterized, :ch, type}

    @impl true
    def init(opts) do
      scale = Keyword.fetch!(opts, :scale)

      (is_integer(scale) and scale >= 0) ||
        raise ArgumentError, ":scale needs to be a non-negative integer"

      {:decimal, unquote(size), scale}
    end

    @impl true
    def cast(value, _type), do: Ecto.Type.cast(:decimal, value)

    @impl true
    def dump(value, _dumper, _type), do: Ecto.Type.dump(:decimal, value)

    @impl true
    def load(value, _loader, _type), do: Ecto.Type.load(:decimal, value)
  end
end
