defmodule Ecto.Integration.TestRepo do
  use Ecto.Repo, adapter: Ecto.Adapters.ClickHouse, otp_app: :ecto_ch

  def create_prefix(db) do
    query!("CREATE DATABASE #{db}")
  end

  def drop_prefix(db) do
    query!("DROP DATABASE #{db}")
  end

  def uuid, do: Ecto.UUID
end
