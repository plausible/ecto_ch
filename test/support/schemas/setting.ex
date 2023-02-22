defmodule EctoClickHouse.Integration.Setting do
  @moduledoc false

  use Ecto.Schema
  import Ecto.Changeset

  @primary_key false
  schema "settings" do
    field :properties, :map
  end

  def changeset(struct, attrs) do
    cast(struct, attrs, [:properties])
  end
end
