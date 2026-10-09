# PostgreSQL Receiver

<!-- markdownlint-disable MD013 -->

- Type: `urn:otel:receiver:postgresql`
- Feature: `postgresql` (explicit opt-in, excluded from `contrib-receivers`)
- Status: Experimental, development-only
- Output: One OTLP log per selected row, with columns in the structured body

**Database transport is unencrypted TCP without server-identity verification.
Use only isolated, trusted development environments and disposable credentials.**
TLS settings are rejected. The engine's `crypto-ring` feature can secure
exporters, but does not encrypt the PostgreSQL connection.

## Getting started

Use the [console example](../../../../../configs/postgresql-console.yaml).
Provision a source table and a least-privilege account with SELECT access, then
adjust the connection, query, result schema, and initial cursor. Set `PG_HOST`,
`PG_USERNAME_FILE`, and `PG_PASSWORD_FILE`; the latter two identify readable
credential files. Provide persistent, writable checkpoint storage.

From `rust/otap-dataflow`:

```sh
cargo run --no-default-features --features postgresql,crypto-ring --bin df_engine -- \
  --config configs/postgresql-console.yaml
```

The example needs no OTLP endpoint and prints selected values locally. Use
appropriate development data; console output is not durable downstream storage.

## Configuration

One receiver owns one query/collecting connection and requires a one-core
pipeline. Unknown fields and null configuration values are rejected.

| Field | Contract |
| --- | --- |
| `source_id` | Required stable name, 1-128 UTF-8 bytes without control characters. |
| `connection.host` | Required single DNS name or IP address; no URI or host list. |
| `connection.port` | Defaults to `5432`; must be nonzero. |
| `connection.database` | Required name, 1-63 UTF-8 bytes. |
| `query.statement` | Required read-only incremental SELECT, at most 16 KiB. |
| `query.interval` | Defaults to `1m`; accepts `1m` through `24h` in whole seconds. `1.5m` is valid. |
| `query.fetch_size_rows` | Defaults to `300`; accepts `1` through `1000`. Byte limits may reduce a fetch further. |
| `query.catch_up.max_pages` | Defaults to `32`; accepts `1` through `1024`, including empty probes. |
| `query.catch_up.max_duration` | Defaults to `10s`; accepts `1ms` through `5min`. Gates the next page, not an active page. |
| `query.result_schema` | Required ordered columns: `name`, native `source_type`, `nullable`, and `type_modifier`. Must match database metadata exactly. |
| `query.output.timestamp_column` | Optional `timestamp`/`timestamptz` event-time column; omission uses observation time. |
| `watermark` | Required composite timestamp and signed-integer tie-breaker, as shown in the example. |
| `checkpoint.directory` | Required persistent storage directory. |

Checkpoint defaults are `on_nack: rewind`, `on_permanent_nack: pause`,
`nack_backoff: 1s`, and `max_consecutive_failures: 5`. The last setting counts
checkpoint-write failures, not connection retries. Interval, fetch-size, and
catch-up changes preserve checkpoint compatibility; source/query/schema changes
may invalidate it. Result-schema configuration is not automatically discovered.

### Authentication

Bind `basic_auth_provider` to the existing
[flat-file provider](../../../../contrib-extensions/src/flat_file_user_pass_auth/README.md),
as shown in the console example. The root `postgresql` feature includes it.
The receiver consumes credentials through the capability, not a custom file
reader or HTTP Basic headers; PostgreSQL authentication uses its native protocol.

The provider refreshes configured files at `password_secret_file_refresh` and
can retain an old password until refresh. Healthy sessions are not replaced;
new connections use the current snapshot. A reconnect before refresh can fail
authentication. Changing the username during a receiver's lifetime is rejected:
stop, review source identity/grants, and restart. After restart, the principal
must still read the same schema-qualified source.

### SQL and cursor contract

Use schema-qualified persistent ordinary tables with aliases and explicit,
qualified column projections. Up to three inner joins are supported when their
equality predicates cover a proven unique key on each joined side. Cursor
columns must be direct, non-null columns of the driving table. The timestamp
and tie-breaker pair must uniquely identify each result position.

Use either keyset below, optionally ANDed with supported static comparisons,
and order by timestamp then tie-breaker ascending:

```sql
(e.ts > :last_timestamp OR
 (e.ts = :last_timestamp AND e.id > :last_tie_breaker))

(e.ts, e.id) > (:last_timestamp, :last_tie_breaker)
```

Native `$1`/`$2` binds are also accepted; do not mix styles. After validating
the original query, execution uses the equivalent tuple keyset and internal
`LIMIT 1000`. Values remain bound parameters, and the original SQL defines
checkpoint compatibility. Ask the database owner to provision an index starting
with the timestamp and tie-breaker; the receiver requires no write privileges.

Timestamp cursors use `timestamp` or `timestamptz`, `timezone: UTC`, and an ISO
8601 initial value with `T` and `Z`, exactly representable at the column's
precision. Numeric offsets and space-separated values are rejected. Tie-breakers
use `int2`, `int4`, or `int8`, with checked ranges.

Views, RLS, partitioned/foreign/temporary tables, system schemas, functions,
computed projections/casts, stars, outer joins, subqueries, grouping,
unrestricted OR, user-supplied LIMIT/OFFSET/FETCH, and multiple statements are
unsupported. Referenced columns, including filter/join columns, are validated.
Catalog checks run every page; cached statements/decoders do not bypass them.
Metadata changes fail closed.

## Value mapping

These are OTLP body mappings, not a Log Analytics custom-table mapping.

| Native type | Output |
| --- | --- |
| `bool`; `int2`, `int4`, `int8` | Boolean; signed 64-bit integer. |
| `numeric` | Exact decimal string, preserving scale. |
| `float4`, `float8` | Finite double; NaN/infinity are rejected. |
| `text`, `varchar`, `bpchar` | Valid UTF-8 string, preserving padding. |
| `bytea` | Bytes. |
| `timestamp`, `timestamptz` | ISO 8601 UTC text: `YYYY-MM-DDTHH:MM:SS.ffffffZ`. Timestamp without timezone is interpreted as UTC. |
| `date`; `uuid` | `YYYY-MM-DD`; hyphenated UUID string. |
| `json`, `jsonb` | Validated JSON text, preserving numeric precision rather than expanding into OTLP fields. |
| `interval` | `months=N;days=N;microseconds=N`. |
| Nullable `NULL` | Empty OTLP value, retaining the column name. |

Unsupported types, invalid values/UTF-8, precision loss, and oversized records
fail explicitly; rows are not silently skipped or truncated. No automatic
unsupported-type-to-string/dynamic fallback is provided. A NULL or out-of-range
optional event timestamp uses observation time while retaining its original
body value. Watermark timestamps remain strictly validated.

## Resource and delivery boundaries

Fixed limits are a 30-second operation budget, 10-second connection budget,
1,000-row pages, separate 8 MiB normalized/encoded budgets, 128 output columns,
1 MiB native rows, and 16,384 characters of numeric output.

Fetches probe one row, then adapt to the configured count and largest observed
row size. Abrupt size increases can require discarding/re-reading part of one
fetch; hard row/page bounds are independent of this estimate. Backend frames
over 8 MiB are rejected before driver body allocation. Checked frames are
forwarded individually with fixed 8 KiB socket read-ahead. These are bounded
buffers, not a process-RSS guarantee.

The Oracle-style catch-up defaults allow at most 32 page fetches or ten seconds
per cycle, then wait `query.interval`; byte limits and backpressure can reduce
progress further. Each page waits for ACK and durable checkpointing.
Confirmed timeouts and transient connection/query failures use shared capped
backoff. Authentication, permissions, invalid data, and unconfirmed cleanup
remain terminal.

The [shared scraper](../../../../scraper/README.md#polling-and-delivery-semantics)
owns scheduling, admission, encoding, feedback, checkpointing, and shutdown.
The adapter isolates driver work on one dedicated thread with a capacity-one
command channel, and ends transactions before waiting for ACK. Unconfirmed
cleanup retains source ownership until process exit.

At-least-once delivery requires stable unique positions, commit-visible ordering,
retained immutable rows, and a meaningful downstream ACK boundary. Late-visible
rows behind the committed cursor, keyless/timestamp-only collection, automatic
tail discovery, TLS, and full production qualification remain unsupported.

## Development checks

From `rust/otap-dataflow`:

```sh
cargo test -p otel-arrow-dfe-contrib-nodes --no-default-features --features postgresql --lib postgresql_receiver
cargo clippy -p otel-arrow-dfe-contrib-nodes --no-default-features --features postgresql --lib --tests -- -D warnings
```
