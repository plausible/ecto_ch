Benchee.run(%{"to_utf8" => fn input -> Ecto.Adapters.ClickHouse.to_utf8(input) end},
  memory_time: 2,
  inputs: %{"small" => "/some/url" <> <<0xAE>> <> "-/"}
)
