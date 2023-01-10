defmodule Dev do
  defmodule Example do
    use Ecto.Schema

    @primary_key false
    schema "example" do
      field :a, Ch.Types.UInt32
      field :b, :string
      field :c, :naive_datetime
      field :d, {:array, :string}
      field :e, {:array, Ch.Types.Int8}
      # or Ch.Types.FixedString, size: 2
      field :f, {:string, 2}
    end
  end
end
