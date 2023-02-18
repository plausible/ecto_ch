defmodule EctoClickHouse.Integration.Migration do
  @moduledoc false

  use Ecto.Migration

  def change do
    create table(:events, primary_key: false, engine: "MergeTree") do
      add :domain, :string
      add :type, :string
      add :tags, {:array, :string}
      add :session_id, :integer, unsigned: true, size: 64, primary_key: true
      timestamps(updated_at: false)
    end

    create table(:sessions, primary_key: false, engine: "MergeTree") do
      add :id, :integer, primary_key: true, unsigned: true, size: 64
      add :domain, :string
      timestamps(updated_at: false)
    end
  end
end
