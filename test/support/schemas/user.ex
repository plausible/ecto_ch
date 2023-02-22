defmodule EctoClickHouse.Integration.User do
  @moduledoc false

  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, Ch.Types.UInt64, autogenerate: false}
  @foreign_key_type Ch.Types.UInt64
  schema "users" do
    field :name, :string

    timestamps()

    many_to_many :accounts, EctoClickHouse.Integration.Account, join_through: "account_users"
  end

  def changeset(struct, attrs) do
    struct
    |> cast(attrs, [:name])
    |> validate_required([:name])
  end
end
