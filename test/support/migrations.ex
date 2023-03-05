defmodule EctoClickHouse.Integration.Migration do
  @moduledoc false

  use Ecto.Migration

  def change do
    create table(:users, primary_key: false, engine: "MergeTree", comment: "users table") do
      add :id, :UInt64, primary_key: true
      add :name, :string, comment: "name column"
      add :custom_id, :uuid
      timestamps()
    end

    create table(:posts, primary_key: false, engine: "MergeTree") do
      add :id, :UInt64, primary_key: true
      add :title, :string
      add :counter, :UInt32
      add :blob, :binary
      add :bid, :binary_id
      add :uuid, :uuid
      # add :meta, :map
      # add :links, {:map, :string}
      # add :intensities, {:map, :float}
      add :public, :boolean
      add :cost, :"Decimal64(2)"
      add :visits, :UInt16
      add :wrapped_visits, :UInt64
      add :intensity, :Float32
      add :author_id, :UInt64
      add :posted, :date
      # timestamps(null: true)
      timestamps()
    end

    create table(:posts_users,
             primary_key: false,
             engine: "MergeTree",
             options: "order by tuple()"
           ) do
      add :post_id, :UInt64
      add :user_id, :UInt64
    end

    create table(:posts_users_pk, primary_key: false, engine: "MergeTree") do
      add :id, :UInt64, primary_key: true
      add :post_id, :UInt64
      add :user_id, :UInt64
      timestamps()
    end

    create table(:permalinks, primary_key: false, engine: "MergeTree") do
      add :id, :UInt64, primary_key: true
      # add :uniform_resource_locator, :string
      add :url, :string
      add :title, :string
      add :post_id, :UInt64
      add :user_id, :UInt64
    end

    create table(:comments, primary_key: false, engine: "MergeTree") do
      add :id, :UInt64, primary_key: true
      add :text, :string
      add :lock_version, :UInt8, default: fragment("1")
      add :post_id, :UInt64
      add :author_id, :UInt64
    end

    create table(:customs, primary_key: false, engine: "MergeTree") do
      add :bid, :binary_id, primary_key: true
      add :uuid, :uuid
    end

    create table(:customs_customs,
             primary_key: false,
             engine: "MergeTree",
             options: "order by tuple()"
           ) do
      add :custom_id1, :binary_id
      add :custom_id2, :binary_id
    end

    create table(:barebones, primary_key: false, engine: "MergeTree") do
      add :id, :UInt64, primary_key: true
      add :num, :integer
    end

    create table(:orders, primary_key: false, engine: "MergeTree") do
      add :id, :UInt64, primary_key: true
      # add :item, :map
      # add :items, :map
      # add :meta, :map
      add :permalink_id, :UInt64
    end

    create table(:tags, primary_key: false, engine: "MergeTree") do
      add :id, :UInt64, primary_key: true
      add :ints, {:array, :Int64}
      add :uuids, {:array, :uuid}
      # add :items, {:array, :map}
    end

    create table(:array_loggings, primary_key: false, engine: "MergeTree") do
      add :id, :UInt64, primary_key: true
      add :uuids, {:array, :uuid}
      timestamps()
    end

    create table(:composite_pk, primary_key: false, engine: "MergeTree") do
      add :a, :UInt64, primary_key: true
      add :b, :UInt64, primary_key: true
      add :name, :string
    end

    create table(:corrupted_pk,
             primary_key: false,
             engine: "MergeTree",
             options: "order by tuple()"
           ) do
      add :a, :string
    end

    create table(:posts_users_composite_pk, primary_key: false, engine: "MergeTree") do
      add :id, :UInt64, primary_key: true
      add :post_id, :UInt64, primary_key: true
      add :user_id, :UInt64, primary_key: true
      timestamps()
    end

    create table(:usecs, primary_key: false, engine: "MergeTree") do
      add :id, :UInt64, primary_key: true
      add :naive_datetime_usec, :naive_datetime_usec
      add :utc_datetime_usec, :utc_datetime_usec
    end

    create table(:loggings, primary_key: false, engine: "MergeTree") do
      add :bid, :binary_id, primary_key: true
      add :int, :UInt64
      add :uuid, :uuid
      timestamps()
    end

    # ---------------------------------------------------------------

    create table(:events, primary_key: false, engine: "MergeTree") do
      add :id, :UInt64, primary_key: true
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
