defmodule Ecto.Adapters.ClickHouse.UTF8Test do
  use ExUnit.Case, async: true
  import Ecto.Adapters.ClickHouse, only: [to_utf8: 1]

  test "escapes invalid utf8 chars with �" do
    path = "/some/url" <> <<0xAE>> <> "-/"
    assert to_utf8(path) == "/some/url�-/"

    path = <<0xAF>> <> "/some/url" <> <<0xAE, 0xFE>> <> "-/" <> <<0xFA>>
    assert to_utf8(path) == "�/some/url�-/�"

    # https://clickhouse.com/docs/en/sql-reference/functions/string-functions/#tovalidutf8
    assert to_utf8("\x61\xF0\x80\x80\x80b") == "a�b"
  end
end
