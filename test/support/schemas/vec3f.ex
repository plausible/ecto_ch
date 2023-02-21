defmodule EctoClickHouse.Integration.Vec3f do
  @moduledoc false

  use Ecto.Schema

  @primary_key false
  schema "vec3f" do
    field :x, Ch.Types.Float64
    field :y, Ch.Types.Float64
    field :z, Ch.Types.Float64
  end
end
