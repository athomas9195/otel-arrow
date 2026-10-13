# PostgreSQL Receiver

<!-- markdownlint-disable MD013 -->

PostgreSQL receiver: `urn:otel:receiver:postgresql`. The opt-in `postgresql` feature is excluded from `contrib-receivers`.

**Database transport is unencrypted TCP without server-identity verification. Use only isolated, trusted development environments and disposable credentials.**

## Getting started

Use the [console example](../../../../../configs/postgresql-console.yaml). Provision a least-privilege account with SELECT access and adjust the schema-qualified source, connection, and initial cursor. Set `PG_HOST`, `PG_USERNAME_FILE`, and `PG_PASSWORD_FILE`; the latter two name readable credential files. Provide persistent, writable checkpoint storage.

From `rust/otap-dataflow`:

```sh
cargo run --no-default-features --features postgresql,crypto-ring --bin df_engine -- \
  --config configs/postgresql-console.yaml
```

No OTLP endpoint is needed for the example. Console output is not durable downstream storage.

## Configuration

One receiver owns one query/collecting connection and requires a one-core pipeline. Unknown fields and null configuration values are rejected.

| Field | Contract |
| --- | --- |
| `source_id` | Required stable name, 1-128 UTF-8 bytes without control characters. |
| `connection.host` | Required single DNS name or IP address; no URI or host list. |
| `connection.port` | Defaults to `5432`; nonzero. |
| `connection.database` | Required name, 1-63 UTF-8 bytes. |
| `query.statement` | Required read-only SELECT, at most 16 KiB. |
| `query.interval` | Default `1m`; `1m`-`24h` in whole seconds (`1.5m` is valid). |
| `query.max_rows_per_poll` | Required page ceiling, `1`-`10000`; no default or whole-cycle row cap. |
| `query.max_batch_bytes` | Required separate normalized-page and encoded-OTLP ceilings, `1` byte-`256 MiB`; integer bytes or strings such as `10 MiB`. |
| `query.fetch_size_rows` | Default `300`; `1`-`10000`, no greater than the page ceiling. Byte estimates may reduce a fetch. |
| `query.catch_up.max_pages` | Default `32`; `1`-`1024`, including empty probes. |
| `query.catch_up.max_duration` | Default `10s`; `1ms`-`5min`. Gates the next page, not an active page. |
| `watermark` | Required composite, typed scalar, or snapshot mode; see below. |
| `checkpoint.directory` | Required persistent storage directory. |

Database, credential, and native statement operations use a fixed 30-second timeout. Connection attempts use a separate 10-second cap.

Checkpoint defaults: `on_nack: rewind`, `on_permanent_nack: pause`, `nack_backoff: 1s`, `max_consecutive_failures: 5`. The last counts checkpoint-write failures, not connection retries. Interval, page/fetch budgets, and catch-up changes preserve checkpoint compatibility; connection, authored SQL, and watermark changes invalidate it. Incompatible checkpoints fail, never silently reset.

Log timestamps follow timestamp watermarks: composite and scalar timestamp modes use their timestamp column; other scalar modes and snapshots use observation time. There is no separate `query.output` setting. Checkpoints from earlier configurable-output builds are incompatible: use a new checkpoint directory and an explicitly reviewed initial cursor.

### Authentication and source identity

Bind `basic_auth_provider` to the [flat-file provider](../../../../contrib-extensions/src/flat_file_user_pass_auth/README.md), included by the root `postgresql` feature. The capability supplies credentials for PostgreSQL's native protocol, not HTTP Basic headers.

Files refresh at `password_secret_file_refresh`; a reconnect before refresh may use the old password and fail. Healthy sessions are not replaced. Username changes during a receiver's lifetime are rejected: stop, review source identity/grants, and restart. After restart the principal must still read the same source.

### SQL and cursor contract

Use schema-qualified persistent ordinary tables, aliases, and explicit qualified projections. Up to three inner joins are allowed; incremental modes require equality predicates covering a proven unique key on each joined side. Cursor columns must be direct, non-null columns of the driving table; composite pairs must uniquely identify each result position.

The at-least-once guarantee also requires cursor-ordered commit visibility: no row may become visible later at or before a committed cursor. Keep cursor and selected values stable, and retain rows unchanged throughout the maximum outage, downstream retry, and checkpoint-recovery window. Backdated inserts, late commits, updates, or early deletion can otherwise be skipped or make a NACK replay differ from the original page.

For composite mode, use either keyset below, optionally ANDed with supported static comparisons, and order by timestamp then tie-breaker ascending:

```sql
(e.ts > :last_timestamp OR
 (e.ts = :last_timestamp AND e.id > :last_tie_breaker))

(e.ts, e.id) > (:last_timestamp, :last_tie_breaker)
```

Native `$1`/`$2` binds are also accepted; do not mix styles.

Timestamp cursors require `timestamp`/`timestamptz`, `timezone: UTC`, and an ISO 8601 initial value with `T`/`Z`, at most six fractional digits, exactly representable at native precision. Numeric offsets, spaces, and precision loss are rejected. Tie-breakers use range-checked `int2`, `int4`, or `int8`.

Views, RLS, partitioned/foreign/temporary tables, system schemas, functions, computed projections/casts, stars, outer joins, subqueries, grouping, unrestricted OR, user LIMIT/OFFSET/FETCH, and multiple statements are unsupported. All referenced columns, including filter/join columns, are validated.

### Scalar and snapshot modes

| Mode / scalar type | Native key contract |
| --- | --- |
| `scalar` / `int64` | Non-null `int2`, `int4`, or `int8`; checked native range. |
| `scalar` / `uint64` | Non-null `numeric(p,0)`, precision `1..20`; values must fit both the declaration and `u64`. Binary binding preserves values above `i64::MAX`. |
| `scalar` / `timestamp` | Non-null `timestamp`/`timestamptz`; UTC `T`/`Z`, at most six fractional digits, exactly representable at native precision. |
| `scalar` / `string` | Non-null `text`/`varchar`, at most 1024 UTF-8 bytes without NUL; catalog-proven deterministic `pg_catalog.C` or `pg_catalog.POSIX` collation. |
| `snapshot` | No cursor, initial value, bind, ordering, or uniqueness requirement; repeats the complete bounded result. |

Scalar keys require a catalog-proven single-column unique index on the driving table and ascending `key > :bind` ordering. String keys require UTF-8 database/connection encoding, verified on every connect and during catalog discovery. UUID, decimal, casts, `bpchar`, locale, unknown/default, and nondeterministic collations are not accepted string keys; `C.UTF8` is not treated as C/POSIX. Body mappings are unchanged.

Replace the console example's query and watermark together:

```yaml
query:
  statement: >
    SELECT e.event_id, e.actor FROM public.receiver_events AS e
    WHERE e.event_id > :last_value ORDER BY e.event_id ASC
  max_rows_per_poll: 10000
  max_batch_bytes: 10 MiB
watermark:
  mode: scalar
  column: event_id
  bind: last_value
  initial: {type: int64, value: 0}
```

Other explicit initial types: `{type: uint64, value: 9223372036854775808}`, `{type: string, value: ""}`, or `{type: timestamp, value: "2026-01-01T00:00:00.000000Z"}`. Types are never inferred from YAML values.

For snapshots, remove the cursor predicate and ordering, keep both page budgets, and use `watermark: {mode: snapshot}`. Nullable/non-unique projections and row-multiplying supported inner joins are allowed. Each interval/restart emits the full result again; this is not CDC and deletions produce no tombstones.

Snapshots use a page-limit-plus-one overflow probe. Row or normalized/encoded-byte overflow pauses the source without emitting a partial prefix; increase the bound or narrow the query before restarting. Catalog/control receive-limit errors and unconfirmed cleanup remain terminal, not successful snapshots.

### Metadata discovery

Local validation derives output names and cursor positions without contacting PostgreSQL. Bounded catalog queries discover native types, modifiers, and nullability before preparing the user query; unsupported types, invalid cursor/event-time columns, and unrepresentable initial values fail closed. Prepared output names, native types/modifiers, and table/column origins must match.

Catalog reads share the operation timeout and receive limits: at most two relation matches, requested attributes plus one, and 129 index descriptions (more than 128 indexes fails). No source-data scan is needed. Every page/reconnect compares fresh metadata with cached signatures; drift fails closed during the receiver's lifetime.

The schema baseline is **not persisted across process restarts**. Keep schema, grants, column semantics, and unique cursor ordering stable across restarts. Checkpoint identity hashes the authored connection/query/watermark and timestamp-mapping contract, not discovered types.

## Value mapping

| Native type | OTLP body value |
| --- | --- |
| `bool`; `int2`, `int4`, `int8` | Boolean; signed 64-bit integer. |
| `numeric` | Exact decimal string preserving scale. |
| `float4`, `float8` | Finite double; NaN/infinity rejected. |
| `text`, `varchar`, `bpchar`; `bytea` | Valid UTF-8 string preserving padding; bytes. |
| `timestamp`, `timestamptz` | `YYYY-MM-DDTHH:MM:SS.ffffffZ`; timestamp without timezone interpreted as UTC. |
| `date`; `uuid` | `YYYY-MM-DD`; hyphenated UUID string. |
| `json`, `jsonb` | Validated JSON text preserving numeric precision, not expanded OTLP fields. |
| `interval`; nullable `NULL` | `months=N;days=N;microseconds=N`; empty OTLP value retaining the column name. |

Invalid/unsupported values, UTF-8 errors, precision loss, and oversized records fail explicitly without skipping, truncation, or automatic type-to-string fallback. Timestamps outside the OTLP time range use observation time while retaining the original body value; watermark timestamps remain strict and non-null.

## Resource and delivery boundaries

The example chooses `10000` rows/`10 MiB`; neither is a default. Other bounds: 10-second connect budget (also limited by operation timeout), 128 output columns, and 16,384 numeric-output characters. Fetching probes one row then adapts to the largest observed row; abrupt size increases may cause re-reading part of a fetch. Geometric page allocation charges metadata, cursors, and spare capacity; hard budgets do not depend on the estimate.

DataRow frames are checked before driver body allocation against `max_batch_bytes + 519` wire-overhead bytes. Only non-DataRow messages get a 64 KiB minimum allowance; catalog DataRows do not. Frames are forwarded individually with fixed 8 KiB read-ahead. Normalized allocations and encoded OTLP are checked separately against the configured byte budget; wire admission does not authorize expansion. There is no independent one-MiB text/bytea cap or total-process RSS guarantee.

The [shared scraper](../../../../scraper/README.md#polling-and-delivery-semantics) owns scheduling, admission, encoding, ACKs, checkpointing, and shutdown. Default catch-up stops after 32 page fetches or ten seconds, then waits the interval; byte budgets/backpressure can reduce progress. Each page waits for ACK and durable checkpointing. Confirmed timeouts/transient failures use shared capped backoff; authentication, permissions, invalid data, and unconfirmed cleanup remain terminal.

Driver work stays on one dedicated thread behind a capacity-one channel. Transactions end before waiting for ACK. Unconfirmed cleanup retains source ownership until process exit. Incremental at-least-once delivery requires stable unique positions, commit-visible ordering, retained immutable rows, and meaningful downstream ACKs. Late-visible rows behind the cursor, automatic tail discovery, TLS, and full production qualification remain unsupported.

## Development

```sh
cargo test -p otel-arrow-dfe-contrib-nodes --no-default-features --features postgresql --lib postgresql_receiver
cargo clippy -p otel-arrow-dfe-contrib-nodes --no-default-features --features postgresql --lib --tests -- -D warnings
```
