# Upstream integration tests

This directory contains tests adapted from the shared adapter integration suites
shipped in the Ecto and Ecto SQL Hex packages:

| Local suite | Upstream source |
| --- | --- |
| `assoc`, `interval`, `joins`, `repo`, `type` | `ecto/integration_test/cases/*.exs` |
| `logging`, `sql`, `subquery` | `ecto_sql/integration_test/sql/*.exs` |

Most suites are copied so we can retain useful assertions while adapting them to
ClickHouse. The upstream files mix supported behavior with tests that assume
transactions, SQL Sandbox, row locks, UPDATE, foreign keys, unique constraints,
or PostgreSQL SQL syntax. Existing upstream feature tags do not cover every
incompatible case, and some suites need repository or schema fixtures we do not
provide. Excluding tests cannot fix fixture errors that occur while a file loads.

Our copies also account for ClickHouse-specific integer types, non-null defaults,
explicit types for schemaless inserts, synchronous delete settings, and unordered
query results. Importing whole upstream files would lose those adjustments or
require skipping useful tests. When updating dependencies, compare the upstream
suites and bring over compatible cases that are not already covered here; keep
their names so future comparisons are straightforward.

`subquery_test.exs` is the exception: its eight existing cases run unchanged, so
it loads the packaged Ecto SQL suite directly. Shared upstream tests run serially
because cleanup truncates shared tables. Three unsupported correlated-subquery
features are excluded by their upstream tags in `test/test_helper.exs`. Imported
test failures must be rerun with the dependency file and line shown in the failure
output; `mix test --failed` does not discover them through the local wrapper.

Project-specific suites belong in [`../adapters/clickhouse/`](../adapters/clickhouse/),
alongside the adapter's other tests. They can still use `Ecto.Integration.Case`
and the shared repository and schemas from `test/support/`.
