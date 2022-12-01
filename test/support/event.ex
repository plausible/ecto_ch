defmodule Event do
  use Ecto.Schema

  @primary_key false
  schema "events" do
    field :name, :string
    field :user_id, :integer
    field :timestamp, :naive_datetime
  end
end
