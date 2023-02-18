defmodule EctoClickHouse.Integration.Session do
  @moduledoc false

  use Ecto.Schema
  import Ecto.Changeset
  alias EctoClickHouse.Integration.Event

  @primary_key {:id, Ch.Types.UInt64, autogenerate: false}
  schema "sessions" do
    field :domain, :string
    has_many :events, Event
    timestamps(updated_at: false)
  end

  def changeset(session, attrs) do
    session
    |> cast(attrs, [:domain])
    |> validate_required([:domain])
  end
end
