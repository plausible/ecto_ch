# Ecto ClickHouse Adapter

[![Documentation badge](https://img.shields.io/badge/Documentation-ff69b4)](https://hexdocs.pm/ecto_ch)
[![Hex.pm badge](https://img.shields.io/badge/Package%20on%20hex.pm-informational)](https://hex.pm/packages/ecto_ch)

Uses [Ch](https://github.com/plausible/ch) as driver.

## Installation

```elixir
defp deps do
  [
    {:ecto_ch, "~> 0.8.0"}
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

Optionally you can also set the default table engine and options to use in migrations

```elixir
config :ecto_ch,
  default_table_engine: "TinyLog",
  default_table_options: [cluster: "little-giant", order_by: "tuple()"]
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

`:settings` option can be used to enable [asynchronous inserts,](https://clickhouse.com/docs/en/optimize/asynchronous-inserts) lightweight [deletes,](https://clickhouse.com/docs/en/guides/developer/lightweght-delete) global [FINAL](https://clickhouse.com/docs/en/operations/settings/settings#final) modifier, and [more:](https://clickhouse.com/docs/en/operations/settings/settings)

```elixir
MyApp.Repo.insert_all(MyApp.Example, rows, settings: [async_insert: 1])
MyApp.Repo.delete_all("example", settings: [allow_experimental_lightweight_delete: 1])
MyApp.Repo.all(MyApp.AggregatedExample, settings: [final: 1])
```

#### Migrations

ClickHouse-specific options can be passed into `table.options` and `index.options`

```elixir
table_options = [cluster: "my-cluster"]
engine_options = [order_by: "tuple()"]
options = table_options ++ engine_options

create table(:posts, primary_key: false, engine: "ReplicatedMergeTree", options: options) do
  add :message, :string
  add :user_id, :UInt64
end
```

is equivalent to

```sql
CREATE TABLE `posts` ON CLUSTER `my-cluster` (
  `message` String,
  `user_id` UInt64
) ENGINE ReplicatedMergeTree ORDER BY tuple()
```

## Caveats

#### [ARRAY JOIN](https://clickhouse.com/docs/en/sql-reference/statements/select/array-join)

For `ARRAY JOIN` examples and other ClickHouse-specific JOIN types please see [clickhouse_joins_test.exs.](./test/ecto/integration/clickhouse_joins_test.exs)

#### NULL

`DEFAULT` expressions on columns are ignored when inserting RowBinary.

[See Ch for more details and an example.](https://github.com/plausible/ch#null-in-rowbinary)

## Benchmarks

[See Ch for benchmarks.](https://github.com/plausible/ch#benchmarks)
