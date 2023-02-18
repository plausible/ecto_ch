defmodule Ecto.Integration.TestRepo do
  use Ecto.Repo, adapter: Ecto.Adapters.ClickHouse, otp_app: :chto

  def uuid, do: Ecto.UUID
end
