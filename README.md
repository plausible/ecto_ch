# Ecto ClickHouse Adapter

[![Hex Package](https://img.shields.io/hexpm/v/chto.svg)](https://hex.pm/packages/chto)
[![Hex Docs](https://img.shields.io/badge/hex-docs-blue.svg)](https://hexdocs.pm/chto)

Uses [Ch](https://github.com/plausible/ch) as driver.

## Installation

```elixir
defp deps do
  [
    {:chto, github: "plausible/chto"}
  ]
end
```

## Usage

In your `config/config.exs`

```elixir
config :my_app, ecto_repos: [MyApp.Repo]
config :my_app, MyApp.Repo, url: "http://username:password@localhost:8123/database"
```

In your application code

```elixir
defmodule MyApp.Repo do
  use Ecto.Repo,
    otp_app: :my_app,
    adapter: Ecto.Adapters.ClickHouse
end
```

#### Ecto schemas

For automatic RowBinary encoding please use the custom `Ch` Ecto type:

```elixir
defmodule MyApp.Example do
  use Ecto.Schema

  @primary_key false
  schema "example" do
    field :number, Ch, type: "UInt32"
    field :name, Ch, type: "String"
    field :maybe_name, Ch, type: "Nullable(String)"
    field :country_code, Ch, type: "FixedString(2)"
    field :price, Ch, type: "Decimal32(2)"
    field :map, Ch, type: "Map(String, UInt64)"
    field :ipv4, Ch, type: "IPv4"
    field :ipv4s, Ch, type: "Array(IPv4)"
    field :enum, Ch, type: "Enum8('hello' = 1, 'world' = 2)"
    # etc.
  end
end

MyApp.Repo.insert_all(MyApp.Example, rows)
```

Note that some base Ecto types like `:string` would also work.

#### Schemaless inserts

For schemaless inserts `:types` option with a mapping of `field->type` needs to be provided

```elixir
types = [
  number: "UInt64",
  # or `number: :u64`
  # or `number: Ch.Types.u64()`
  # etc.
]

MyApp.Repo.insert_all("example", rows, types: types)
```

#### Settings

`:settings` option can be used to enable [asynchronous inserts,](https://clickhouse.com/docs/en/optimize/asynchronous-inserts) lightweght [deletes,](https://clickhouse.com/docs/en/guides/developer/lightweght-delete) and [more](https://clickhouse.com/docs/en/operations/settings/settings)

```elixir
MyApp.Repo.insert_all(MyApp.Example, rows, settings: [async_insert: 1])
MyApp.Repo.delete_all("example", settings: [allow_experimental_lightweight_delete: 1])
```

## Caveats

#### [ARRAY JOIN](https://clickhouse.com/docs/en/sql-reference/statements/select/array-join)

For now `:inner_lateral` and `:left_lateral` are used for `ARRAY` and `LEFT ARRAY` joins:

```elixir
"arrays_test"
|> join(:inner_lateral, [a], r in "arr")
|> select([a, r], {a.s, r.arr})
```

is equivalent to

```sql
SELECT a0."s", a1."arr"
FROM "arrays_test" AS a0
ARRAY JOIN "arr" AS a1
```

#### NULL

`DEFAULT` expressions on columns are ignored when inserting RowBinary.

[See Ch for more details and an example.](https://github.com/plausible/ch#null-in-rowbinary)

## Benchmarks

[See Ch for benchmarks.](https://github.com/plausible/ch#benchmarks)
