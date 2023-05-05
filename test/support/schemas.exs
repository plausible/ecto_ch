defmodule EctoClickHouse.Integration.AccountUser do
  @moduledoc false

  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, Ch, type: "UInt64"}
  schema "account_users" do
    timestamps()

    belongs_to :account, EctoClickHouse.Integration.Account, type: Ch, raw: "UInt64"
    belongs_to :user, EctoClickHouse.Integration.User, type: Ch, raw: "UInt64"
  end

  def changeset(struct, attrs) do
    struct
    |> cast(attrs, [:account_id, :user_id])
    |> validate_required([:account_id, :user_id])
  end
end

defmodule EctoClickHouse.Integration.Account do
  @moduledoc false

  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, Ch, type: "UInt64"}
  schema "accounts" do
    field :name, :string
    field :email, :string

    timestamps()

    many_to_many :users, EctoClickHouse.Integration.User, join_through: "account_users"
    has_many :products, EctoClickHouse.Integration.Product
  end

  def changeset(struct, attrs) do
    struct
    |> cast(attrs, [:name])
    |> validate_required([:name])
  end
end

defmodule EctoClickHouse.Integration.Product do
  @moduledoc false

  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, Ch, type: "UInt64"}
  schema "products" do
    field :name, :string
    field :description, :string
    field :external_id, Ecto.UUID
    field :tags, {:array, :string}, default: []
    field :approved_at, :naive_datetime
    field :price, Ch, type: "Decimal64(2)"

    belongs_to :account, EctoClickHouse.Integration.Account, type: Ch, raw: "UInt64"

    timestamps()
  end

  def changeset(struct, attrs) do
    struct
    |> cast(attrs, [:name, :description, :tags, :account_id, :approved_at])
    |> validate_required([:name])
    |> maybe_generate_external_id()
  end

  defp maybe_generate_external_id(changeset) do
    if get_field(changeset, :external_id) do
      changeset
    else
      put_change(changeset, :external_id, Ecto.UUID.generate())
    end
  end
end

defmodule EctoClickHouse.Integration.Setting do
  @moduledoc false

  use Ecto.Schema
  import Ecto.Changeset

  @primary_key false
  schema "settings" do
    field :properties, :map
  end

  def changeset(struct, attrs) do
    cast(struct, attrs, [:properties])
  end
end

defmodule EctoClickHouse.Integration.User do
  @moduledoc false

  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, Ch, type: "UInt64"}
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

defmodule EctoClickHouse.Integration.Vec3f do
  @moduledoc false

  use Ecto.Schema

  @primary_key false
  schema "vec3f" do
    field :x, Ch, type: "Float64"
    field :y, Ch, type: "Float64"
    field :z, Ch, type: "Float64"
  end
end
