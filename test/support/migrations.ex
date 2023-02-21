defmodule EctoClickHouse.Integration.Migration do
  @moduledoc false

  use Ecto.Migration

  def change do
    create table(:events, primary_key: false, engine: "MergeTree") do
      add :domain, :string
      add :type, :string
      add :tags, {:array, :string}
      add :session_id, :UInt64, primary_key: true
      timestamps(updated_at: false)
    end

    create table(:sessions, primary_key: false, engine: "MergeTree") do
      add :id, :UInt64, primary_key: true
      add :domain, :string
      timestamps(updated_at: false)
    end

    create table(:accounts, primary_key: false, engine: "MergeTree") do
      add :id, :UInt64, primary_key: true
      add :name, :string
      # TODO support collate nocase
      add :email, :string, collate: :nocase
      timestamps()
    end

    create table(:users, primary_key: false, engine: "MergeTree") do
      add :id, :UInt64, primary_key: true
      add :name, :string
      add :custom_id, :uuid
      timestamps()
    end

    create table(:account_users, primary_key: false, engine: "MergeTree") do
      add :id, :UInt64, primary_key: true
      add :account_id, :UInt64
      add :user_id, :UInt64
      add :role, :string
      timestamps()
    end

    create table(:products, primary_key: false, engine: "MergeTree") do
      add :id, :UInt64, primary_key: true
      add :account_id, :UInt64
      add :name, :string
      add :description, :text
      add :external_id, :uuid
      add :tags, {:array, :string}
      add :approved_at, :naive_datetime
      add :price, :"Decimal64(2)"
      timestamps()
    end

    create table(:vec3f, primary_key: false, engine: "MergeTree", options: "order by tuple()") do
      add :x, :Float64
      add :y, :Float64
      add :z, :Float64
    end

    # TODO
    # create table(:settings, primary_key: false, engine: "MergeTree") do
    #   add :properties, :JSON
    # end
  end
end
