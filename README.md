# Ecto ClickHouse Adapter

[![Documentation badge](https://img.shields.io/badge/Documentation-ff69b4)](https://hexdocs.pm/ecto_ch)
[![Hex.pm badge](https://img.shields.io/badge/Package%20on%20hex.pm-informational)](https://hex.pm/packages/ecto_ch)

Uses [Ch](https://github.com/plausible/ch) as driver.

## Installation

```elixir
defp deps do
  [
    {:ecto_ch, "~> 0.11.0"}
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

#### UTF-8 text and binary data

ClickHouse `String` columns can store arbitrary bytes. To replace invalid UTF-8 when loading text into an Ecto schema, define a custom type in your application:

```elixir
defmodule MyApp.Utf8String do
  use Ecto.Type

  @impl true
  def type, do: :string

  @impl true
  def cast(value), do: Ecto.Type.cast(:string, value)

  @impl true
  def dump(value), do: Ecto.Type.dump(:string, value)

  @impl true
  def load(value) when is_binary(value) do
    sanitized =
      value
      |> String.chunk(:valid)
      |> Enum.map_join(fn chunk ->
        if String.valid?(chunk), do: chunk, else: "�"
      end)

    {:ok, sanitized}
  end

  def load(_value), do: :error
end
```

This replaces each consecutive run of invalid bytes with one `�`, matching Ch's current sanitizer. It uses public Elixir functions available on Elixir 1.15 and does not depend on Ch's sanitization helper. On Elixir 1.16+, [`String.replace_invalid/1`](https://hexdocs.pm/elixir/String.html#replace_invalid/2) is a shorter alternative, although it can produce a different number of replacement characters.

Use this type for text and `:binary` for payloads such as protobufs:

```elixir
defmodule MyApp.Event do
  use Ecto.Schema

  @primary_key false
  schema "events" do
    field :message, MyApp.Utf8String
    field :payload, :binary
  end
end
```

Ecto invokes the custom type's `load/1` even though its underlying type is `:string`. Built-in `:string` fields accept any binary without UTF-8 validation. The custom type above sanitizes reads; its `cast/1` and `dump/1` leave bytes unchanged.

Ch 0.9.x currently sanitizes `String` columns before Ecto loads them, including fields declared as `:binary`. To preserve payload bytes with these versions, explicitly select raw decoding for every result column:

```elixir
import Ecto.Query

query = from e in MyApp.Event, select: {e.message, e.payload}
MyApp.Repo.all(query, types: [:binary, :binary])
```

Here Ch preserves both values, then `MyApp.Utf8String` sanitizes only `message`. The `:types` list must match every selected SQL column in order; this override is for buffered queries, not `Repo.stream/2`. The custom type will continue to sanitize text if Ch switches to preserving raw bytes by default. See [issue #177](https://github.com/plausible/ecto_ch/issues/177).

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

Column options like `:default`, `:null`, and `:comment` are supported as well, and work in `create`, `add`, and `modify`:

```elixir
create table(:posts, primary_key: false, engine: "MergeTree", options: [order_by: "tuple()"]) do
  add :user_id, :UInt64, comment: "ClickPipes source"
end
```

is equivalent to

```sql
CREATE TABLE `posts` (
  `user_id` UInt64 COMMENT 'ClickPipes source'
) ENGINE MergeTree ORDER BY tuple()
```

## Caveats

#### FORMAT clauses in SELECT queries

Starting with [ClickHouse 26.8](https://github.com/ClickHouse/ClickHouse/pull/105249), the `X-ClickHouse-Format` request header takes precedence over a `FORMAT` clause in a `SELECT` query. Since Ch always sends this header, use the `:format` query option to request a custom response format instead of including `FORMAT` in the SQL:

```elixir
Repo.query!("SELECT * FROM events", [], format: "JSONCompact")
```

This does not affect `INSERT ... FORMAT ...` input formats.

#### [ALTER TABLE ... UPDATE](https://clickhouse.com/docs/en/sql-reference/statements/alter/update)

ClickHouse doesn't support `UPDATE` statements as of now, so `Repo.update/2` and `Repo.update_all/3` raise when called. But `Repo.alter_update_all/3` -- which executes `ALTER TABLE ... UPDATE` -- can be used instead.

Note that `ALTER TABLE ... UPDATE` is considered an admin operation and comes with a performance cost. Please read https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse for more information.

For examples, please see [clickhouse_alter_update_test.exs.](./test/ecto/integration/clickhouse_alter_update_test.exs)

#### [ARRAY JOIN](https://clickhouse.com/docs/en/sql-reference/statements/select/array-join)

For `ARRAY JOIN` examples and other ClickHouse-specific JOIN types please see [clickhouse_joins_test.exs.](./test/ecto/integration/clickhouse_joins_test.exs)

#### NULL

`DEFAULT` expressions on columns are ignored when inserting RowBinary.

[See Ch for more details and an example.](https://github.com/plausible/ch#null-in-rowbinary)

## Benchmarks

[See Ch for benchmarks.](https://github.com/plausible/ch#benchmarks)
