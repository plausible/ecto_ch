defmodule EctoClickHouse.Integration.Product do
  @moduledoc false

  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, Ch.Types.UInt64, autogenerate: false}
  @foreign_key_type Ch.Types.UInt64
  schema "products" do
    field :name, :string
    field :description, :string
    field :external_id, Ecto.UUID
    field :tags, {:array, :string}, default: []
    field :approved_at, :naive_datetime
    field :price, Ch.Types.Decimal, precision: 18, scale: 2

    belongs_to :account, EctoClickHouse.Integration.Account

    timestamps()
  end

  def changeset(struct, attrs) do
    struct
    |> cast(attrs, [:name, :description, :tags, :account_id, :approved_at])
    |> validate_required([:name])
    |> maybe_generate_external_id()
  end

  defp maybe_generate_external_id(changeset) do
    if get_field(changeset, :external_id) do
      changeset
    else
      put_change(changeset, :external_id, Ecto.UUID.generate())
    end
  end
end
