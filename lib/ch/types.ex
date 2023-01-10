basic_types = [
  {:u8, UInt8, :integer},
  {:u16, UInt16, :integer},
  {:u32, UInt32, :integer},
  {:u64, UInt64, :integer},
  {:i8, Int8, :integer},
  {:i16, Int16, :integer},
  {:i32, Int32, :integer},
  {:i64, Int64, :integer},
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

# TODO
defmodule Ch.Types.FixedString do
  use Ecto.ParameterizedType
end
