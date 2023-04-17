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

## Caveats

#### Ecto schemas

For automatic RowBinary encoding some schema fields need to use custom types:

```elixir
defmodule MyApp.Example do
  use Ecto.Schema

  @primary_key false
  schema "example" do
    field :numeric_types_need_size, Ch.Types.UInt32
    field :no_custom_type_for_strings, :string
    field :datetime, :naive_datetime
    field :maybe_name, Ch.Types.Nullable, type: :string
    field :country_code, Ch.Types.FixedString, size: 2
    field :price, Ch.Types.Decimal32, scale: 2
  end
end

MyApp.Repo.insert_all(MyApp.Example, rows)
```

#### Schemaless inserts

For schemaless inserts `:types` is required

```elixir
types = [
  numeric_types_need_size: :u32,
  no_custom_type_for_strings: :string,
  datetime: :datetime,
  maybe_name: {:nullable, :string},
  country_code: {:string, _size = 2},
  price: {:decimal, _size = 32, _scale = 2}
]

MyApp.Repo.insert_all("example", rows, types: types)
```

#### Settings

`:settings` option can be used to enable [asynchronous inserts,](https://clickhouse.com/docs/en/optimize/asynchronous-inserts) lightweght [deletes,](https://clickhouse.com/docs/en/guides/developer/lightweght-delete) and [more](https://clickhouse.com/docs/en/operations/settings/settings)

```elixir
MyApp.Repo.insert_all(MyApp.Example, rows, settings: [async_insert: 1])
MyApp.Repo.delete_all("example", settings: [allow_experimental_lightweight_delete: 1])
```

#### [ARRAY JOIN](https://clickhouse.com/docs/en/sql-reference/statements/select/array-join)

`:inner_lateral` and `:left_lateral` join types are used for `ARRAY JOIN` and `LEFT ARRAY JOIN` until Ecto adds `:array_join` types.

`ARRAY JOIN` example:

```elixir
"arrays_test"
|> join(:inner_lateral, [a], r in "arr", on: true)
|> select([a, r], {a.s, r.arr})
```

```sql
SELECT a0."s", a1."arr"
FROM "arrays_test" AS a0
ARRAY JOIN "arr" AS a1
```

#### NULL

`DEFAULT` expressions on columns are ignored when inserting RowBinary.

[See Ch for more details and an example.](https://github.com/plausible/ch#null-in-rowbinary)

#### UTF-8

Both `:binary` and `:string` schema fields are decoded as UTF-8 since Ecto [doesn't call adapter's loaders for base types](https://github.com/elixir-ecto/ecto/blob/b5682bbd2123d32760af664cc3f91c5d8174ef74/lib/ecto/type.ex#L891-L897) like `:binary` and `:string`.

[See Ch for more details and an example.](https://github.com/plausible/ch#utf-8-in-rowbinary)

## Benchmarks

[See Ch for benchmarks.](https://github.com/plausible/ch#benchmarks)
