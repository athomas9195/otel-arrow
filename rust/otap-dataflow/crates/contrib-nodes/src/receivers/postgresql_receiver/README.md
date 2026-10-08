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
- `test.rs`: configuration, adapter, codec, and protocol regression coverage.

PostgreSQL-specific helpers remain separate: `query.rs` validates SQL and catalog
provenance, `value.rs` decodes exact native values, and `transport.rs` enforces
backend frame bounds. These helpers do not implement another scheduler.

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
      username: collector
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

The timestamp must be `timestamp` or `timestamptz`, with an initial UTC `...Z`
value exactly representable by its native precision. Tie-breakers use
`int2`, `int4`, or `int8` with checked binding ranges. Only composite watermarks
are currently supported; the shared scalar API is explicitly rejected.

Functions, computed projections, stars, outer joins, subqueries, grouping,
unrestricted OR, LIMIT/OFFSET/FETCH, and multiple statements are rejected.
Catalog checks run before preparation and on every page. Metadata changes
fail closed. Use least-privilege grants and immutable retained source rows.

## Resource and delivery boundaries

The experiment's fixed limits are retained: 30-second operation budget,
10-second connection budget, 300-row fetch groups, 1,000-row pages, separate
8 MiB normalized/encoded budgets, 128 output columns, and 1 MiB backend frames.
NUMERIC output is limited to 16,384 characters. They are not an RSS guarantee.
The plaintext frame guard remains in place despite removing TLS.

Values retain exact integer, decimal, binary, timestamp, and JSON text semantics.
Nonfinite floats/numerics, invalid UTF-8, unsupported native types, lossy cursor
precision, and oversized records fail explicitly. No silent truncation occurs.

At-least-once delivery requires stable unique composite positions, commit-visible
ordering, retained immutable rows, and an appropriate downstream ACK boundary.
Failed or unacknowledged work does not advance checkpoints. Retries and permanent
rejection use the shared controller policies. Unconfirmed cleanup retains source
ownership until process exit. Late-visible rows behind a committed cursor, automatic
tail discovery, keyless ties, and full product qualification remain out of scope.

## Build and tests

From `rust/otap-dataflow`:

To print rows to the console, provision the source table and password file,
then adjust the username, connection, query, result schema, and initial cursor
in the example to match your database:

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
cargo clippy -p otel-arrow-dfe-contrib-nodes --no-default-features --features postgresql --lib --tests -- -D warnings
```

For the opt-in live test, provision PostgreSQL 15 in an isolated network, load
`fixture.sql` from this receiver directory, and grant the `collector` role
SELECT access. Set `PG_PASSWORD_FILE` to its password file and `PG_HOST` to the
fixture server. No CA or username-file environment variables are used.
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
