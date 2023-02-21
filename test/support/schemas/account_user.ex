defmodule EctoClickHouse.Integration.AccountUser do
  @moduledoc false

  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, Ch.Types.UInt64, autogenerate: false}
  @foreign_key_type Ch.Types.UInt64
  schema "account_users" do
    timestamps()

    belongs_to :account, EctoClickHouse.Integration.Account
    belongs_to :user, EctoClickHouse.Integration.User
  end

  def changeset(struct, attrs) do
    struct
    |> cast(attrs, [:account_id, :user_id])
    |> validate_required([:account_id, :user_id])
  end
end
