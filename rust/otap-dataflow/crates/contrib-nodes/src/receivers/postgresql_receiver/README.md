# PostgreSQL Receiver

<!-- markdownlint-disable MD013 -->

## Metadata

- Type: `urn:otel:receiver:postgresql`
- Feature: `postgresql` (explicit opt-in, excluded from `contrib-receivers`)
- Stability: Experimental, development-only
- Output: OTLP logs, one record per selected row
- Execution: One query and collecting connection per receiver, one pipeline core

## Transport limitation

**PostgreSQL TLS is not implemented in this integration. The database connection
uses unencrypted TCP with no server-certificate verification. Use only isolated,
trusted development environments and disposable credentials.**

TLS configuration is rejected rather than silently ignored. The engine's
`crypto-ring` feature can secure downstream exporters but does not encrypt the
PostgreSQL connection. This receiver does not meet production requirements for
encrypted database connections.

## Architecture

The implementation uses the same factory/capability/shared-scraper structure as
the Oracle receiver:

- `mod.rs`: feature-gated registration, required authentication binding, and source ownership.
- `config.rs`: native configuration and checkpoint compatibility identity.
- `adapter.rs`: local capability consumer, operation cancellation, worker proxy, and redacted errors.
- `worker.rs`: connection/session lifecycle, read-only transactions, and portal pages.
- `test.rs`: configuration, adapter, and codec regression coverage.

PostgreSQL-specific helpers remain separate: `query.rs` validates SQL and catalog
provenance and `value.rs` decodes exact native values. PostgreSQL protocol
decoding is handled by `tokio-postgres`, behind a bounded frame reader that
checks message lengths before exposing their headers to the driver.

The existing shared scraper owns scheduling, memory-pressure admission, OTLP
encoding, ACK/NACK handling, checkpoints, and shutdown deadlines. One dedicated
worker thread isolates driver I/O and decoding; a capacity-one command channel
prevents an unbounded queue. Database transactions end before waiting for ACK.

## Configuration

Start with the [PostgreSQL-to-console example](../../../../../configs/postgresql-console.yaml).
It prints collected rows locally and needs no OTLP endpoint.
The top-level receiver fields follow Oracle's layout:

| Field | Meaning |
| --- | --- |
| `source_id` | Required source name, 1-128 UTF-8 bytes without control characters. |
| `connection.host` | Required single DNS name or IP address. No URI or host list. |
| `connection.port` | Defaults to 5432; must be nonzero. |
| `connection.database` | Required database name, 1-63 UTF-8 bytes. |
| `query.statement` | Required read-only incremental SELECT, at most 16 KiB. |
| `query.interval` | Defaults to `1m`; `1m` through `24h`, whole seconds. |
| `query.fetch_size_rows` | Defaults to `300`, matching Oracle. Accepts `1` through `1000`, bounded by PostgreSQL's fixed page size. The byte budget may reduce a fetch further. |
| `query.catch_up.max_pages` | Defaults to `32`; accepts `1` through `1024`. Maximum page fetches in one cycle, including empty probes. |
| `query.catch_up.max_duration` | Defaults to `10s`; accepts `1ms` through `5m`. Elapsed cycle budget for starting another page. |
| `query.result_schema` | Required ordered output columns: `name`, `source_type`, `nullable`, and native `type_modifier`. |
| `query.output.timestamp_column` | Optional event-time column; omission uses the shared observed-time fallback. |
| `watermark` | Required composite timestamp and signed-integer cursor. |
| `checkpoint` | Required persistent checkpoint directory and optional shared feedback policies. |

The ordered result schema is retained from the experiment and must exactly
match the source metadata. It is not automatically discovered into configuration.
Unknown fields, null configuration values, unsupported scalar watermark modes,
and obsolete nested `query.watermark`/`query.sql` settings are rejected.

Checkpoint defaults are `on_nack: rewind`, `on_permanent_nack: pause`,
`nack_backoff: 1s`, and `max_consecutive_failures: 5`. The last setting concerns
checkpoint-write failures, not a global retry limit.

### Authentication

Bind the required `basic_auth_provider` capability to an existing credential
extension, just as for Oracle:

```yaml
extensions:
  pg-credentials:
    type: urn:otel:extension:flat_file_user_pass_auth
    config:
      username_file: /run/secrets/postgresql/username
      password_secret_file: /run/secrets/postgresql/password
      password_secret_file_refresh: 1m
nodes:
  pg:
    type: urn:otel:receiver:postgresql
    capabilities:
      basic_auth_provider: pg-credentials
    # Add the receiver config shown in the full example.
```

The root `postgresql` feature bundles the flat-file provider. The adapter does
not read credential files or construct HTTP Basic headers. It obtains an owned
credential snapshot on the local runtime and passes it to the database worker.
Acquisition is bounded and cancellable, and provider error details are redacted.
The capability's username/password validation restrictions apply.

The examples set `PG_USERNAME_FILE` and `PG_PASSWORD_FILE` to files containing
the two credential values. Trailing CR/LF characters are removed. The shared
provider rereads either file on `password_secret_file_refresh`; inline
`username` remains supported when `username_file` is not configured.

New connections use the current provider snapshot. Healthy sessions are not
replaced merely because credentials refresh. A changed username during a
receiver's lifetime is rejected; stop and review the source identity before
changing principals. Password changes do not change checkpoint identity.
After restart, operators must ensure the new principal still reads the same
schema-qualified source; credentials are not part of the configuration fingerprint.

The provider can cache an old password until its next refresh. A reconnect
before refresh may therefore fail authentication. This integration does not
promise immediate adoption of a changed secret or implement a secret-store resolver.

### SQL and cursor contract

Use schema-qualified persistent ordinary tables with aliases and explicit
qualified column projections. Up to three inner joins are accepted when their
equality predicates cover a proven unique key on each joined side. Cursor
columns must be direct non-null columns of the driving table. Views, RLS,
partitioned/foreign/temporary tables, and system schemas remain unsupported.

Use `(ts > :last_timestamp OR (ts = :last_timestamp AND id > :last_tie_breaker))`
or a tuple comparison, optionally ANDed with supported static comparisons.
Native `$1`/`$2` parameters are also accepted; do not mix styles.
Parameters are compiled without interpolating cursor values into SQL.
ORDER BY must be timestamp then tie-breaker ascending.

After validation, execution uses the equivalent tuple keyset
`(ts, id) > ($1, $2)` and an internal 1,000-row SQL limit. This lets PostgreSQL
plan an indexed, bounded page instead of repeatedly scanning or sorting the
remaining backlog. The original configured SQL still defines checkpoint
compatibility; operator-authored LIMIT clauses remain unsupported.

For sustained collection, have the database owner provision an index starting
with the timestamp and tie-breaker columns in that order. Verify representative
early and late cursor positions with PostgreSQL's query-plan tools, particularly
for joins and additional filters. The receiver does not create indexes or
require write privileges.

The timestamp must be `timestamp` or `timestamptz`, with an initial UTC `...Z`
value exactly representable by its native precision. Tie-breakers use
`int2`, `int4`, or `int8` with checked binding ranges. Only composite watermarks
are currently supported; the shared scalar API is explicitly rejected.

Use ISO 8601 UTC text, for example `2026-10-09T10:00:00.123456Z`.
Cursor configuration requires the `T` separator and `Z` suffix; space-separated
timestamps and numeric timezone offsets are rejected. The `timezone: UTC`
field remains required for configuration compatibility.

Both PostgreSQL `timestamp` and `timestamptz` values are emitted as
`YYYY-MM-DDTHH:MM:SS.ffffffZ`, including cursor/checkpoint values. A native
`timestamp` has no offset and is interpreted as UTC, not as the host's local
time. A native `timestamptz` represents an instant and is emitted in UTC.
Microsecond precision is preserved. Calendar `date` values and timestamp-like
strings inside text or JSON columns keep their existing representations.

Functions, computed projections, stars, outer joins, subqueries, grouping,
unrestricted OR, LIMIT/OFFSET/FETCH, and multiple statements are rejected.
Catalog checks run before preparation and on every page, using prepared catalog
statements and a bounded attribute lookup per relation rather than one lookup
per column. Metadata changes fail closed. Use least-privilege grants and
immutable retained source rows.

Native column decoders are cached in the compiled plan. Cursor ordering uses
the already decoded native timestamp and integer, avoiding per-row timestamp
reparsing and previous-cursor string copies. Checkpoint parsing, timestamp
precision validation, and the shared page-order validation remain enforced.
A single-entry timestamp conversion cache reuses the formatted value for
timestamp ties. Its storage stays bounded even when every row has a different
timestamp; it does not cache query results or skip source reads.

## Resource and delivery boundaries

The fixed per-page limits are retained: 30-second operation budget,
10-second connection budget, 1,000-row pages, separate
8 MiB normalized/encoded budgets, and 128 output columns. The value decoder
rejects rows over 1 MiB after the driver has received them; NUMERIC output is
limited to 16,384 characters.

Fetch groups default to 300 rows and can be configured with `query.fetch_size_rows`,
as in Oracle. The first fetch probes one row. Subsequent fetch counts use the
configured row limit, largest normalized row size observed so far, and remaining
page-byte budget. This avoids repeatedly receiving hundreds of large rows that
cannot fit in a page.
The estimate is retained across pages; abrupt row-size increases can still
require discarding part of one outstanding fetch and rereading it from the
acknowledged cursor. Actual row/page limits are checked independently of the
estimate.

A backend frame over 8 MiB is rejected before the driver receives its header
or allocates its body. Frames are delivered individually so the driver's
bounded response queue cannot accumulate an entire fetch as one large batch.
An 8 KiB socket buffer amortizes small reads without exposing unchecked frame
headers to the driver; its read-ahead capacity does not depend on message size.
This is a byte-boundary check, not a second PostgreSQL decoder or a
process-RSS guarantee. The existing 1 MiB row and 8 MiB page checks remain
in place; an oversized row is an explicit error, never truncated or skipped.

Catch-up uses the same configurable policy as Oracle: by default a cycle
admits at most 32 page fetches or ten seconds of elapsed time. It also ends
when the query returns no rows or admission is interrupted. The next cycle
waits for `query.interval`. The duration budget gates the next page; it does
not interrupt a page already in progress.

Each page still waits for downstream ACK and durable checkpointing. Page and
byte limits, memory pressure, cancellation, and shutdown remain enforced.
With 1,000-row pages, the default page budget allows at most 32,000 rows per
cycle; byte limits, elapsed time, or backpressure can reduce that number.
Both catch-up settings can be adjusted without invalidating checkpoints.

Confirmed query timeouts wait a full configured collection interval before
retrying. Temporary connection-slot exhaustion, database unavailability,
deadlocks, and serialization failures use capped availability backoff.
Broken-pipe connection failures also retry after cleanup. Authentication,
permission, invalid-data, and unconfirmed-cleanup errors remain
terminal.

Transient database failures interrupt the current catch-up cycle, matching
Oracle's shared scheduling behavior. Memory-pressure admission remains active.

Values retain exact integer, decimal, binary, timestamp, and JSON text semantics.
Nonfinite floats/numerics, invalid UTF-8, unsupported native types, lossy cursor
precision, and oversized records fail explicitly. No silent truncation occurs.
For an optional output event-time column, NULL or a valid timestamp outside
OTLP's unsigned nanosecond range uses observation time; the original value
remains in the record body. Watermark timestamps remain strictly validated.

At-least-once delivery requires stable unique composite positions, commit-visible
ordering, retained immutable rows, and an appropriate downstream ACK boundary.
Failed or unacknowledged work does not advance checkpoints. Retries and permanent
rejection use the shared controller policies. Unconfirmed cleanup retains source
ownership until process exit. Late-visible rows behind a committed cursor, automatic
tail discovery, keyless ties, and full product qualification remain out of scope.

## Build and tests

From `rust/otap-dataflow`:

### Load generator

The [`postgresql_load_generator` example](../../../examples/postgresql_load_generator.rs)
provides the same `--rows`, `--collision-size`, and `--reset` options as Oracle's
generator. It creates `public.receiver_events` with deterministic data and a
cursor index. `--collision-size` controls how many rows share a timestamp;
existing IDs are left unchanged. Defaults are 1,000 rows and groups of 10.
Rows are generated inside PostgreSQL rather than collected in a client-side list.

Use an **isolated, disposable database**. Set `PG_HOST`, `PG_DATABASE`,
`PG_USERNAME`, and `PG_PASSWORD_FILE`; `PG_PORT` defaults to 5432.
The database must already exist. The generator account needs table-creation
and write permissions; it is separate from the receiver's SELECT-only role.
Connections are unencrypted, just like the current receiver.

```sh
cargo run -p otel-arrow-dfe-contrib-nodes --no-default-features \
  --features postgresql --example postgresql_load_generator -- \
  --rows 10000 --collision-size 100
```

The optional `--reset` flag **drops and recreates public.receiver_events**.
Never use it on production data. Use a fresh fixture or reset when changing the
collision size; rerunning without reset does not modify existing rows.
Reapply the receiver's SELECT grant after reset. Load rows before starting the
receiver, and use fresh checkpoint state when replaying a reset fixture.
This is a data-setup tool, not a throughput benchmark or delivery assertion.

### Console example

To print rows to the console, provision the source table and both credential
files, then set `PG_USERNAME_FILE` and `PG_PASSWORD_FILE` and adjust the
connection, query, result schema, and initial cursor
in the example to match your database. The generator's schema and timestamp
origin match the supplied example; use database `receiver_fixture` and grant
the `collector` role SELECT access:

```sh
cargo run --no-default-features --features postgresql,crypto-ring --bin df_engine -- \
  --config configs/postgresql-console.yaml
```

The console prints the selected database values; use appropriate development
data. Its output is not a durable downstream ingestion guarantee.

For development checks and the separate OTLP E2E runner:

```sh
cargo build --no-default-features --features postgresql,otlp,crypto-ring --bin df_engine
cargo test -p otel-arrow-dfe-contrib-nodes --no-default-features --features postgresql --lib postgresql_receiver
cargo test -p otel-arrow-dfe-contrib-nodes --no-default-features --features postgresql --example postgresql_load_generator
cargo clippy -p otel-arrow-dfe-contrib-nodes --no-default-features --features postgresql --lib --tests -- -D warnings
```

### Live E2E runner

For the opt-in live test, provision PostgreSQL 15 in an isolated network with
database `receiver_fixture`. Load `fixture.sql` from this receiver directory,
or use the generator with **exactly** `--rows 2305 --collision-size 2305 --reset`
to produce the values expected by the runner. Grant the `collector` role
SELECT access afterward. Set `PG_USERNAME_FILE` to a file containing `collector`,
`PG_PASSWORD_FILE` to its password file, and `PG_HOST` to the fixture server.
The supplied receiver YAML uses port 5432; no CA configuration is used.
The [OTLP fixture configuration](../../../../../configs/postgresql-otlp.yaml)
is retained for this runner, which creates its own temporary capture endpoint.

```sh
cargo run -p otel-arrow-dfe-contrib-nodes --no-default-features \
  --features postgresql --example postgresql_e2e -- \
  target/debug/df_engine configs/postgresql-otlp.yaml
```

The runner launches the real engine with a loopback OTLP capture and validates
2,305 row IDs, typed values, and replay after a crash before ACK. Missing
infrastructure fails explicitly; this is not a performance or TLS qualification.

Unlike Oracle's in-repository `emits_oracle_rows_when_live_test_is_enabled`
smoke test, which checks that a live receiver emits at least one record into
the test runtime, this runner launches the real engine and exercises an OTLP
exporter and crash/replay. Neither load generator performs those assertions.
