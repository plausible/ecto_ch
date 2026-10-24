# Fast inserts

For high insert throughput, send fewer, larger batches to ClickHouse. Start with
`Repo.insert_all/3`; use streaming for large enumerables, server-side asynchronous
inserts when producers cannot batch, or an application buffer when you need to
combine many small writes before sending them.

## Batch ordinary inserts first

Each synchronous insert into a MergeTree table creates new data parts, which
ClickHouse later merges. Many tiny inserts make that background work expensive.
ClickHouse [recommends batches of at least 1,000 rows, ideally 10,000–100,000](https://clickhouse.com/docs/optimize/bulk-inserts).
These are starting points: tune for row size, memory, throughput, and acceptable
time until data is queryable. At low traffic, a timer may need to flush a smaller
batch to meet your latency budget.

The examples below assume a configured `MyApp.Repo` using this adapter and this
table and schema:

```elixir
MyApp.Repo.query!("""
  CREATE TABLE events (site_id UInt64, name String)
  ENGINE = MergeTree ORDER BY (site_id, name)
""")

defmodule MyApp.Event do
  use Ecto.Schema

  @primary_key false
  schema "events" do
    field :site_id, Ch, type: "UInt64"
    field :name, Ch, type: "String"
  end
end
```

Batch maps rather than calling `Repo.insert/2` for each event:

```elixir
rows = Enum.map(1..10_000, &%{site_id: 1, name: "event-#{&1}"})

MyApp.Repo.insert_all(MyApp.Event, rows, settings: [async_insert: 0])

# Without an Ecto schema:
MyApp.Repo.insert_all("events", rows,
  types: [site_id: "UInt64", name: "String"],
  settings: [async_insert: 0]
)
```

These are alternative ways to insert the same rows; running both inserts them
twice. The explicit `async_insert: 0` selects client-side batching regardless of
server or user defaults.

`insert_all/3` already uses `RowBinaryWithNamesAndTypes` over HTTP. You do not need
to build SQL value lists or serialize maps to JSON. ClickHouse documents
[RowBinary as an efficient row-oriented input format](https://clickhouse.com/docs/best-practices/selecting-an-insert-strategy).
Keep types explicit, especially for integers and decimals whose Ecto types do not
identify a unique ClickHouse type.

Batch size alone does not determine part count. An insert can create parts in
each partition it touches, and parts never merge across partitions. Avoid
[high-cardinality partition keys](https://clickhouse.com/docs/best-practices/choosing-a-partitioning-key)
such as an event ID. Measure part creation across all producers, including other
application nodes. ClickHouse's recommendation of roughly one synchronous insert
per second is a starting heuristic for keeping merge work manageable, not a
fixed throughput limit.

## Stream large inputs

For an import that is too large to hold in a list, use the adapter's
`Repo.insert_stream/3`:

```elixir
rows = Stream.map(1..100_000, &%{site_id: 1, name: "event-#{&1}"})

MyApp.Repo.insert_stream(MyApp.Event, rows,
  chunk_every: 1_000,
  settings: [async_insert: 0],
  timeout: 60_000
)
```

This sends one chunked HTTP request. `:chunk_every` controls how many rows the
client encodes at a time; it does not create a separate insert for each chunk or
set the size of ClickHouse's on-disk parts. A long import holds a connection for
the request's duration. Use finite batches instead if you need smaller retry
units or more frequent progress checkpoints.

Unlike `insert_all/3`, `insert_stream/3` skips Ecto preprocessing, including type
dumping and ID generation. Supply all schema fields with values the driver can
encode, including any IDs and timestamps. For a schemaless stream, pass an
ordered `:types` keyword list as in the previous example.

## Let ClickHouse batch small requests

If producers cannot assemble large batches, use
[asynchronous inserts](https://clickhouse.com/docs/optimize/asynchronous-inserts):

```elixir
MyApp.Repo.insert_all(MyApp.Event, [%{site_id: 1, name: "pageview"}],
  settings: [async_insert: 1, wait_for_async_insert: 1],
  timeout: 30_000
)
```

ClickHouse combines compatible requests in memory and flushes according to size,
time, or query-count thresholds. Buffers are separate for different query shapes,
settings, and receiving nodes; keep the inserted columns and settings consistent.
Data becomes queryable after a flush.

Keep `wait_for_async_insert: 1`: the call waits for the flush and receives flush
errors. With `0`, success only acknowledges server-side buffering; accepted data
can be lost and later errors are not returned to the caller. Set the options
explicitly rather than depending on version-specific defaults.

The Elixir call still blocks while waiting. A serial loop of single-row requests
therefore waits for a flush on every iteration. Application batching can still
help; if using concurrent producers, bound their concurrency to the Repo pool and
the server's capacity. Set client timeouts to allow for pool checkout, the async
flush interval, and insertion work. Check the settings supported by your deployed
ClickHouse version before tuning flush thresholds.

## Buffer and encode in the application

An application buffer can combine events from many callers and move encoding out
of the process that writes batches. Plausible uses this approach in its
[event buffer](https://github.com/plausible/analytics/blob/master/lib/plausible/event/write_buffer.ex)
and [shared ingestion buffer](https://github.com/plausible/analytics/blob/master/lib/plausible/ingestion/write_buffer.ex):
prepare the column order and types, encode events at the producer, accumulate
iodata, and flush by byte size or time.

The following small example uses the public `Ch.RowBinary.encode_rows/2` API.
It buffers at most 10 MiB of encoded payload in its state and flushes every second
or before accepting a row that would exceed that size. These limits are example
values, not recommended settings for every workload.

This is a **memory-only buffer**. A successful `push/1` means accepted into local
memory, not persisted. Process or machine failure loses pending events. Use a
durable queue or replayable source when you must recover accepted data.

```elixir
defmodule MyApp.EventBuffer do
  use GenServer
  require Logger

  @types ["UInt64", "String"]
  @sql "INSERT INTO events (site_id, name) FORMAT RowBinary"
  @max_bytes 10 * 1024 * 1024
  @flush_interval 1_000

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  def push(%{site_id: site_id, name: name}) do
    # Encode in the caller, before sending a message to the buffer.
    data = Ch.RowBinary.encode_rows([[site_id, name]], @types)
    data = IO.iodata_to_binary(data)

    if byte_size(data) > @max_bytes do
      {:error, :row_too_large}
    else
      GenServer.call(__MODULE__, {:push, data}, :infinity)
    end
  end

  def flush do
    GenServer.call(__MODULE__, :flush, :infinity)
  end

  @impl true
  def init(nil) do
    schedule_flush()
    {:ok, %{buffer: [], bytes: 0, error: nil}}
  end

  @impl true
  def handle_call({:push, data}, _from, %{error: nil} = state) do
    state =
      if state.bytes + byte_size(data) > @max_bytes,
        do: write(state),
        else: state

    if state.error do
      # The new row was not accepted; the failed batch stays unchanged.
      {:reply, {:error, state.error}, state}
    else
      state = %{state | buffer: [data | state.buffer], bytes: state.bytes + byte_size(data)}
      {:reply, :ok, state}
    end
  end

  def handle_call({:push, _data}, _from, state) do
    {:reply, {:error, state.error}, state}
  end

  def handle_call(:flush, _from, state) do
    state = write(state)
    reply = if state.error, do: {:error, state.error}, else: :ok
    {:reply, reply, state}
  end

  @impl true
  def handle_info(:flush, state) do
    # A failed batch requires an explicit flush/0 to retry it.
    state = if state.error, do: state, else: write(state)
    schedule_flush()
    {:noreply, state}
  end

  defp write(%{bytes: 0} = state), do: state

  defp write(state) do
    try do
      MyApp.Repo.query!(@sql, Enum.reverse(state.buffer),
        command: :insert,
        encode: false,
        settings: [async_insert: 0],
        timeout: 15_000
      )

      %{state | buffer: [], bytes: 0, error: nil}
    rescue
      error ->
        Logger.error("Event buffer flush failed: #{Exception.message(error)}")
        %{state | error: error}
    end
  end

  defp schedule_flush do
    Process.send_after(self(), :flush, @flush_interval)
  end
end
```

Start the buffer after the Repo in your application's supervision tree:

```elixir
children = [MyApp.Repo, MyApp.EventBuffer]
Supervisor.start_link(children, strategy: :one_for_one)
```

Producers can then call:

```elixir
:ok = MyApp.EventBuffer.push(%{site_id: 1, name: "pageview"})
:ok = MyApp.EventBuffer.flush()
```

The SQL column order, `@types`, and encoded values must match the table exactly.
`FORMAT RowBinary` takes rows without a names-and-types header. `encode: false`
tells Ch that the payload is already encoded. This path bypasses Ecto validation,
type dumping, and autogenerated fields; convert custom Ecto types yourself before
encoding. Plausible's
[session buffer](https://github.com/plausible/analytics/blob/master/lib/plausible/session/write_buffer.ex)
illustrates such a conversion. Do not use undocumented encoder helpers just to
cache type normalization; they can change between driver releases.

The example writes synchronously in the GenServer, so only one insert is in flight
and callers wait during a flush. The byte limit covers buffered payload, not the
mailbox, producer memory, or total BEAM memory. Bound producer concurrency and row
size; `GenServer.call/3` alone does not impose a mailbox limit. The timer can also
be delayed by a write or queued messages. If this process becomes a bottleneck,
profile before adding ETS, multiple buffers, or parallel flush workers: each adds
ownership, ordering, and failure-handling work, and more buffers can create smaller
ClickHouse batches.

On a rescued insert error, the buffer retains the batch, logs the failure, and
rejects new rows. It does not retry automatically. Investigate the error and the
deduplication requirements below before calling `flush/0` again; an invalid row
requires intervention rather than repeated retries. An `{:error, reason}` response
from `push/1` means that new row was not accepted. Other failures can still crash
the process, losing its memory. During orderly shutdown, stop producers, call
`flush/0`, handle its result, then stop the buffer and Repo. A termination callback
cannot guarantee delivery after a crash or forced shutdown.

## Plan retries and measure the whole path

A timeout or lost response does not prove an insert failed. Before retrying,
review ClickHouse's [insert deduplication requirements](https://clickhouse.com/docs/guides/developer/deduplicating-inserts-on-retries)
for your server version and table engine. Deduplication needs an enabled query
setting and a table deduplication log with a sufficient window. Plain `MergeTree`
tables, including this guide's example, do not keep that log by default; a primary
key does not enforce uniqueness. Retrying this example can create duplicates.
Keep batch contents, order, and settings stable on retry. Asynchronous insert
deduplication and dependent materialized view behavior differ between versions;
do not assume a recent server's guarantees apply to older deployments.

Benchmark with realistic row widths, partition distribution, materialized views,
and all application nodes writing. Measure rows and bytes per second, time from
acceptance to queryability, encoding CPU, memory, Repo queue time, flush errors,
and buffer or mailbox growth. Larger batches are only useful if they meet your
latency and memory budget. Presorting by the table's `ORDER BY` key may reduce
server work, but include the client's sorting cost in the measurement.

On ClickHouse, inspect active part counts in
[`system.parts`](https://clickhouse.com/docs/operations/system-tables/parts)
and merge activity, rather than only measuring HTTP request latency. When async
insert logging is configured,
[`system.asynchronous_insert_log`](https://clickhouse.com/docs/operations/system-tables/asynchronous_insert_log)
records flush outcomes and exceptions. System tables are node-local: inspect all
receiving nodes when diagnosing a cluster.
