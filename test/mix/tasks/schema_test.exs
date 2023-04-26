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

    test "prints a schema" do
      schema =
        capture_io(fn ->
          Mix.Tasks.Ecto.Ch.Schema.run(["system.numbers"])
        end)

      assert schema == """
             schema "numbers" do
               field :"number", Ch, type: "UInt64"
             end
             """
    end
  end
end
