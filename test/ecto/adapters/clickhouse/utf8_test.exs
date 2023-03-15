defmodule Ecto.Adapters.ClickHouse.UTF8Test do
  use ExUnit.Case, async: true
  import Ecto.Adapters.ClickHouse, only: [to_utf8: 1]

  test "escapes invalid utf8 chars with �" do
    path = "/some/url" <> <<0xAE>> <> "-/"
    assert to_utf8(path) == "/some/url�-/"

    path = <<0xAF>> <> "/some/url" <> <<0xAE, 0xFE>> <> "-/" <> <<0xFA>>
    assert to_utf8(path) == "�/some/url��-/�"
  end
end
