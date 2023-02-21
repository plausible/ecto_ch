defmodule EctoClickHouse.Integration.Account do
  @moduledoc false

  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, Ch.Types.UInt64, autogenerate: false}
  @foreign_key_type Ch.Types.UInt64
  schema "accounts" do
    field :name, :string
    field :email, :string

    timestamps()

    many_to_many :users, EctoClickHouse.Integration.User, join_through: "account_users"
    has_many :products, EctoClickHouse.Integration.Product
  end

  def changeset(struct, attrs) do
    struct
    |> cast(attrs, [:name])
    |> validate_required([:name])
  end
end
