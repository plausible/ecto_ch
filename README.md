# Ecto ClickHouse Adapter

[![Hex Package](https://img.shields.io/hexpm/v/ecto_ch.svg)](https://hex.pm/packages/ecto_ch)
[![Hex Docs](https://img.shields.io/badge/hex-docs-blue.svg)](https://hexdocs.pm/ecto_ch)

Uses [Ch](https://github.com/plausible/ch) as driver.

## Installation

```elixir
defp deps do
  [
    {:ecto_ch, "~> 0.1.0"}
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

Optionally you can also set the default table engine to use in migrations

```elixir
config :ecto_ch, default_table_engine: "TinyLog"
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
    field :ipv4s, {:array, Ch}, type: "IPv4"
    field :enum, Ch, type: "Enum8('hello' = 1, 'world' = 2)"
    # etc.
  end
end

MyApp.Repo.insert_all(MyApp.Example, rows)
```

Some Ecto types like `:string`, `:date`, and `Ecto.UUID` would also work. Others like `:decimal`, `:integer` are ambiguous and should not be used.

[`ecto.ch.schema`](https://hexdocs.pm/ecto_ch/Mix.Tasks.Ecto.Ch.Schema.html) mix task can be used to generate a schema from an existing ClickHouse table.

#### Schemaless inserts

For schemaless inserts `:types` option with a mapping of `field->type` needs to be provided:

```elixir
types = [
  number: "UInt32",
  # or `number: :u32`
  # or `number: Ch.Types.u32()`
  # etc.
]

MyApp.Repo.insert_all("example", rows, types: types)
```

#### Settings

`:settings` option can be used to enable [asynchronous inserts,](https://clickhouse.com/docs/en/optimize/asynchronous-inserts) lightweght [deletes,](https://clickhouse.com/docs/en/guides/developer/lightweght-delete) and [more:](https://clickhouse.com/docs/en/operations/settings/settings)

```elixir
MyApp.Repo.insert_all(MyApp.Example, rows, settings: [async_insert: 1])
MyApp.Repo.delete_all("example", settings: [allow_experimental_lightweight_delete: 1])
```

## Caveats

#### [ARRAY JOIN](https://clickhouse.com/docs/en/sql-reference/statements/select/array-join)

Since [`v3.10.2`](https://github.com/elixir-ecto/ecto/blob/40133ace8c71f1f81432858e71d3d73527f85107/CHANGELOG.md?plain=1#L10) Ecto supports `:array` and `:left_array` join types:

```elixir
from a in "arrays_test", array_join: r in "arr", select: {a.s, r}
```

For an earlier Ecto version `:inner_lateral` and `:left_lateral` join types can be used instead:

```elixir
from a in "arrays_test", inner_lateral_join: r in "arr", select: {a.s, r}
```

Both of these queries are equivalent to:

```sql
SELECT a0."s", a1 FROM "arrays_test" AS a0 ARRAY JOIN "arr" AS a1
```

#### NULL

`DEFAULT` expressions on columns are ignored when inserting RowBinary.

[See Ch for more details and an example.](https://github.com/plausible/ch#null-in-rowbinary)

## Benchmarks

[See Ch for benchmarks.](https://github.com/plausible/ch#benchmarks)
