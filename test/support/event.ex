defmodule EctoClickHouse.Integration.Event do
  @moduledoc false

  use Ecto.Schema
  import Ecto.Changeset
  alias EctoClickHouse.Integration.Session

  @primary_key false
  schema "events" do
    field :domain, :string
    field :type, :string
    field :tags, {:array, :string}
    belongs_to :session, Session, type: Ch.Types.UInt64
    timestamps(updated_at: false)
  end

  def changeset(event, attrs) do
    event
    |> cast(attrs, [:domain, :type])
    |> validate_required([:domain, :type])
  end
end
