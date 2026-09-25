# PostgreSQL receiver

This receiver is currently pre-1.0. Its public API may evolve between minor
releases.

Collects an ordered PostgreSQL query as OTLP logs using the shared
`otel-arrow-dfe-scraper` runtime.

| Item | Value |
| --- | --- |
| Component | `urn:otel:receiver:postgresql` |
| Feature | `postgresql-receiver` (explicit opt-in, outside the contrib umbrella) |
| Database | PostgreSQL 15 is the initial integration-test target |
| Output | One OTLP log per row; selected columns form a typed key-value body |
| Ownership | One query/session per receiver and one core for its pipeline |
| Tracking | [PostgreSQL receiver issue][issue], [database receiver RFC][rfc] |

The adapter owns PostgreSQL syntax, typed bindings, metadata, transport,
cancellation and scalar conversion. The scraper owns scheduling, memory
admission, OTLP encoding, ACK/NACK handling, checkpointing and telemetry.
No second scheduler, exporter or checkpoint implementation is introduced.

## Build and run

From `rust/otap-dataflow`:

```sh
cargo build --locked --no-default-features --features postgresql-receiver,otlp,crypto-ring --bin df_engine
```

Choose the engine's appropriate crypto provider for your environment. The
example above selects ring. The driver is native Rust and does not require
libpq or another separately installed database client.

Use [`configs/postgresql-otlp.yaml`][example] and set:

```sh
export POSTGRES_HOST=database.example.com
export POSTGRES_DATABASE=events
export POSTGRES_CA_FILE=/run/secrets/postgres-ca.pem
export POSTGRES_USERNAME_FILE=/run/secrets/postgres-user
export POSTGRES_PASSWORD_FILE=/run/secrets/postgres-password
export POSTGRES_INITIAL_TIMESTAMP=2026-09-25T00:00:00Z
export OTLP_HTTP_ENDPOINT=https://collector.example.com
cargo run --locked --no-default-features --features postgresql-receiver,otlp,crypto-ring --bin df_engine -- --config configs/postgresql-otlp.yaml
```

Set the initial timestamp deliberately to your desired collection boundary;
the timestamp above is illustrative, not automatic activation-time discovery.
The tie-breaker is exclusive within that timestamp. A saved checkpoint takes
precedence on restart. Windows users can set the same variables with
`$env:NAME = "value"` and use Windows file paths.

## Authentication and TLS

TLS is mandatory. Supply a PEM CA bundle in `connection.tls.ca_file`; the driver
verifies the chain and the configured host's identity. There is no plaintext,
certificate-bypass, hostname-bypass or arbitrary connection-string option.
Port defaults to 5432. Use a hostname present in the server certificate.

Credential files contain UTF-8 text. One terminal newline is removed, but
password spaces are preserved. Each file is limited to 4096 bytes. Do not put
passwords in YAML, command arguments or SQL examples. Protect credential and
checkpoint files with filesystem permissions. Credential files are reread when
a new connection is established, not automatically on an existing session.

A dedicated account needs connection/schema access and SELECT on each referenced
table; administrative privileges are not required. For example, after securely
provisioning the login:

```sql
GRANT CONNECT ON DATABASE events TO collector;
GRANT USAGE ON SCHEMA public TO collector;
GRANT SELECT ON public.events TO collector;
```

Do not grant write privileges or access to side-effecting functions. Parsed
SELECT validation and read-only transactions are defense in depth, not an
audit of database roles or every callable function.

## Configuration and query contract

| Field | Default / constraints |
| --- | --- |
| `source_id` | Required, 1-256 bytes; identifies this checkpoint stream |
| `connection.host`, `database`, `tls.ca_file` | Required |
| `connection.port` | 5432; nonzero |
| `authentication.username_file`, `password_file` | Required paths; no inline credentials |
| `query.statement` | Exactly one supported PostgreSQL SELECT, at most 64 KiB |
| `query.interval` | 1 minute; 1 minute-24 hours, whole seconds |
| `query.timeout` | 30 seconds; 1 second-5 minutes, whole seconds |
| `query.fetch_size_rows` | 300; positive and no greater than the page row limit |
| `query.max_rows_per_poll` | 10,000; maximum 10,000 |
| `query.max_batch_bytes` | 10 MiB; shared limit validation applies |
| `query.catch_up` | Shared defaults: 32 pages / 10 seconds |
| `watermark`, `checkpoint` | Shared composite cursor and checkpoint configuration |

Use `$1` for the timestamp and `$2` for the signed integer tie-breaker:

```sql
SELECT event_ts, event_id, payload FROM public.events
WHERE (event_ts > $1 OR (event_ts = $1 AND event_id > $2))
ORDER BY event_ts ASC, event_id ASC
```

The shared watermark's `bind` strings are logical labels; their values are
bound to these fixed PostgreSQL positions. They are not substituted into SQL.
Initial timestamps must use RFC3339 with an explicit offset and at most
microsecond precision; offsets are normalized to UTC. Timestamp columns
without time zones are interpreted as UTC, not the collector's local zone.

Supported AND filters and inner joins may surround the complete keyset
predicate. SQL is parsed once, not checked with string searches. Statements,
cursor predicates, ordering and live metadata are validated before collection.
Do not alias a different expression into a cursor column.

Cursor columns must be direct, non-null source columns with catalog provenance:
timestamp/timestamptz plus smallint/integer/bigint. Output names must be distinct.
The ordered pair must be stable and unique in the query result; a database
primary key is not inspected or required, but source uniqueness remains the
operator's responsibility.

CTEs, subqueries, set operations, outer joins, explicit LIMIT/OFFSET/FETCH,
row locks and SELECT INTO are rejected. General static binds and computed
cursor expressions are not supported.

## Values and resource bounds

Booleans and integers retain their scalar types. Numeric values preserve exact
decimal text, including scale. Finite floats remain floating point. Text must be
valid UTF-8. Bytes remain bytes; UUID/date/JSON/JSONB/interval values are encoded
as text. Timestamps become UTC text with microseconds. SQL NULL is preserved
except in cursor columns. Arrays, composite/domain/custom types, non-finite
numbers and unrepresentable timestamps fail explicitly.

A prepared statement is reused on one async session. Each page opens a read-only
transaction and fetches a bounded portal; it never materializes the whole result
set. Normalization accounts allocated row/value capacity and cursor text. A
byte-limited prefix does not advance past omitted rows. An oversized single row
fails rather than being truncated.

The PostgreSQL wire driver must receive a row before its normalized size can be
checked. Its protocol buffers, one boundary row and downstream buffers are not
part of the retained-page byte budget. The limits are not a total RSS guarantee.

An operation deadline includes connection setup and page work; server-side
statement timeout is also set for fetching. Cancellation sends a PostgreSQL
cancel request and waits for rollback behind the active operation. Cleanup has
a separate bounded wait; failed confirmation does not authorize lease release.
The protocol pump is one async task, not a blocking pool or per-row task.

## Delivery and current boundaries

Only a matching downstream ACK and successful checkpoint install advance
progress. Retryable NACKs replay; permanent NACKs stop the receiver without
skipping the rejected rows. The engine's recovery policy may restart failed
pipelines; set `policies.runtime_recovery.enabled: false` for terminal pipeline
failure without those restarts.

At-least-once behavior is conditional on commit-visible ordering, immutable
cursor/row values, retained source rows and an appropriate ACK boundary.
Timestamp polling cannot recover arbitrary late commits behind the saved
cursor. This is not CDC, partitioned polling or exactly-once delivery.

The receiver inherits completion-based intervals, bounded catch-up, query-error
termination and no normal-operation ACK deadline from the shared runtime.
Automatic tail discovery, keyless timestamp-tie recovery, late-commit overlap/
deduplication, next-interval transient query retries, UI/secret-store providers
and destination-specific table mapping require separate runtime/product work.
The adapter does not claim those capabilities.

## Tests

Unit and shared-runtime tests do not require a database:

```sh
cargo test --locked -p otel-arrow-dfe-contrib-nodes --features postgresql-receiver --lib postgresql_receiver
cargo test --locked -p otel-arrow-dfe-scraper
cargo clippy --locked -p otel-arrow-dfe-contrib-nodes --features postgresql-receiver --all-targets -- -D warnings
```

The real database test requires Python 3 (standard library only), Docker, and
the engine built above. From `rust/otap-dataflow`:

```sh
docker pull postgres:15-bookworm
python3 crates/contrib-nodes/tests/postgresql_e2e.py --binary target/debug/df_engine
```

On Windows use `py -3` and `target\debug\df_engine.exe`. If `CARGO_TARGET_DIR`
is set, pass the executable under that directory instead.

The test creates a uniquely named disposable PostgreSQL container, generated
CA/server certificates, random credential files and a SELECT-only database
account. Ports bind only to loopback. It verifies typed output, timestamp ties,
portal/page bounds, HTTP 503 replay, ACK checkpointing, process restart,
permanent HTTP rejection, untrusted CA/hostname/password failures, timeout,
oversized rows and nullable cursor rejection. Container, credentials and
temporary state are removed in `finally`; an assertion failure exits nonzero.
It is a correctness
test, not a throughput benchmark or full database/OS support matrix.

[issue]: https://github.com/open-telemetry/otel-arrow/issues/4166
[rfc]: https://github.com/open-telemetry/otel-arrow/issues/3918
[example]: ../../../../../configs/postgresql-otlp.yaml
