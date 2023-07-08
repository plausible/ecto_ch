defmodule Mix.Tasks.Ecto.Ch.SchemaTest do
  use ExUnit.Case
  import ExUnit.CaptureIO

  test "run/1 help" do
    help =
      capture_io(fn ->
        Mix.Tasks.Ecto.Ch.Schema.run([])
      end)

    assert help == """
           Shows an Ecto schema hint for a ClickHouse table.

           Examples:

               $ mix ecto.ch.schema
               $ mix ecto.ch.schema system.numbers
               $ mix ecto.ch.schema system.numbers --repo MyApp.Repo

           """
  end

  describe "run/1" do
    setup do
      put_env_reset(:ecto_ch, :ecto_repos, [Ecto.Integration.TestRepo])
    end

    test "system.numbers" do
      schema =
        capture_io(fn ->
          Mix.Tasks.Ecto.Ch.Schema.run(["system.numbers"])
        end)

      assert schema == """
             @primary_key false
             schema "numbers" do
               field :number, Ch, type: "UInt64"
             end
             """
    end

    test "system.users" do
      schema =
        capture_io(fn ->
          Mix.Tasks.Ecto.Ch.Schema.run(["system.users"])
        end)

      assert schema == """
             @primary_key false
             schema "users" do
               field :name, :string
               field :id, Ecto.UUID
               field :storage, :string
               field :auth_type, Ch, type: "Enum8('no_password' = 0, 'plaintext_password' = 1, 'sha256_password' = 2, 'double_sha1_password' = 3, 'ldap' = 4, 'kerberos' = 5, 'ssl_certificate' = 6)"
               field :auth_params, :string
               field :host_ip, {:array, :string}
               field :host_names, {:array, :string}
               field :host_names_regexp, {:array, :string}
               field :host_names_like, {:array, :string}
               field :default_roles_all, Ch, type: "UInt8"
               field :default_roles_list, {:array, :string}
               field :default_roles_except, {:array, :string}
               field :grantees_any, Ch, type: "UInt8"
               field :grantees_list, {:array, :string}
               field :grantees_except, {:array, :string}
               field :default_database, :string
             end
             """
    end

    test "products" do
      schema =
        capture_io(fn ->
          Mix.Tasks.Ecto.Ch.Schema.run(["products"])
        end)

      assert schema == """
             @primary_key false
             schema "products" do
               field :id, Ch, type: "UInt64"
               field :account_id, Ch, type: "UInt64"
               field :name, :string
               field :description, :string
               field :external_id, Ecto.UUID
               field :tags, {:array, :string}
               field :approved_at, Ch, type: "DateTime"
               field :price, Ch, type: "Decimal(18, 2)"
               field :inserted_at, Ch, type: "DateTime"
               field :updated_at, Ch, type: "DateTime"
             end
             """
    end
  end

  describe "run/1 custom repo flags" do
    test "-r" do
      schema =
        capture_io(fn ->
          Mix.Tasks.Ecto.Ch.Schema.run(["events", "-r", "Ecto.Integration.TestRepo"])
        end)

      assert schema == """
             @primary_key false
             schema "events" do
               field :id, Ch, type: "UInt64"
               field :domain, :string
               field :type, :string
               field :tags, {:array, :string}
               field :session_id, Ch, type: "UInt64"
               field :inserted_at, Ch, type: "DateTime"
               field :event, :string
               field :value, Ch, type: "UInt64"
               field :description, :string
             end
             """
    end

    test "--repo" do
      schema =
        capture_io(fn ->
          Mix.Tasks.Ecto.Ch.Schema.run(["accounts", "--repo", "Ecto.Integration.TestRepo"])
        end)

      assert schema == """
             @primary_key false
             schema "accounts" do
               field :id, Ch, type: "UInt64"
               field :name, :string
               field :email, :string
               field :inserted_at, Ch, type: "DateTime"
               field :updated_at, Ch, type: "DateTime"
             end
             """
    end
  end

  test "build_type/1" do
    import Mix.Tasks.Ecto.Ch.Schema, only: [build_field: 2]

    assert build_field("metric", "String") ==
             ~s[field :metric, :string]

    assert build_field("metric", "Array(String)") ==
             ~s[field :metric, {:array, :string}]

    assert build_field("metric", "Array(UInt64)") ==
             ~s[field :metric, {:array, Ch}, type: "UInt64"]

    assert build_field("metric", "Array(Array(UInt64))") ==
             ~s[field :metric, {:array, {:array, Ch}}, type: "UInt64"]

    assert build_field("metric", "Array(Array(String))") ==
             ~s[field :metric, {:array, {:array, :string}}]

    assert build_field("metric", "Array(Tuple(String, UInt64))") ==
             ~s[field :metric, {:array, Ch}, type: "Tuple(String, UInt64)"]
  end

  defp put_env_reset(app, key, value) do
    prev = Application.get_env(app, key)
    :ok = Application.put_env(app, key, value)

    on_exit(fn ->
      if prev do
        Application.put_env(app, key, prev)
      else
        Application.delete_env(app, key)
      end
    end)
  end
end
