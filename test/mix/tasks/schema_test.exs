defmodule Mix.Tasks.Ecto.Ch.SchemaTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureIO

  describe "run/1" do
    test "prints help" do
      help =
        capture_io(fn ->
          Mix.Tasks.Ecto.Ch.Schema.run([])
        end)

      assert help == """
             Shows an Ecto schema hint for a table.

             Examples:

                 $ mix ecto.ch.schema
                 $ mix ecto.ch.schema system.numbers

             """
    end

    test "prints system.numbers schema" do
      schema =
        capture_io(fn ->
          Mix.Tasks.Ecto.Ch.Schema.run(["system.numbers"])
        end)

      assert schema == """
             @primary_key false
             schema "numbers" do
               field :"number", Ch, type: "UInt64"
             end
             """
    end

    test "prints system.users schema" do
      schema =
        capture_io(fn ->
          Mix.Tasks.Ecto.Ch.Schema.run(["system.users"])
        end)

      assert schema == """
             @primary_key false
             schema "users" do
               field :"name", :string
               field :"id", Ecto.UUID
               field :"storage", :string
               field :"auth_type", Ch, type: "Enum8('no_password' = 0, 'plaintext_password' = 1, 'sha256_password' = 2, 'double_sha1_password' = 3, 'ldap' = 4, 'kerberos' = 5, 'ssl_certificate' = 6)"
               field :"auth_params", :string
               field :"host_ip", {:array, :string}
               field :"host_names", {:array, :string}
               field :"host_names_regexp", {:array, :string}
               field :"host_names_like", {:array, :string}
               field :"default_roles_all", Ch, type: "UInt8"
               field :"default_roles_list", {:array, :string}
               field :"default_roles_except", {:array, :string}
               field :"grantees_any", Ch, type: "UInt8"
               field :"grantees_list", {:array, :string}
               field :"grantees_except", {:array, :string}
               field :"default_database", :string
             end
             """
    end
  end

  test "build_type/1" do
    import Mix.Tasks.Ecto.Ch.Schema, only: [build_field: 2]

    assert build_field("metric", "String") ==
             ~s[field :"metric", :string]

    assert build_field("metric", "Array(String)") ==
             ~s[field :"metric", {:array, :string}]

    assert build_field("metric", "Array(UInt64)") ==
             ~s[field :"metric", {:array, Ch}, type: "UInt64"]

    assert build_field("metric", "Array(Array(UInt64))") ==
             ~s[field :"metric", {:array, {:array, Ch}}, type: "UInt64"]

    assert build_field("metric", "Array(Array(String))") ==
             ~s[field :"metric", {:array, {:array, :string}}]

    assert build_field("metric", "Array(Tuple(String, UInt64))") ==
             ~s[field :"metric", {:array, Ch}, type: "Tuple(String, UInt64)"]
  end
end
