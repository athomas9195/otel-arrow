# PostgreSQL Receiver

## Metadata

| Field | Value |
| --- | --- |
| Type | `receiver:postgresql` |
| URN | `urn:otel:receiver:postgresql` |
| Feature | `postgresql-receiver` (host and contrib crate) |
| Stability | Experimental; PostgreSQL 15; logs |

## Overview

One local receiver owns one query, one source identity, and one collecting
connection. Place it on exactly one pipeline core. The shared scraper retains
ownership of admission, polling, OTLP encoding, checkpoint writes, and downstream
ACK/NACK handling. No portal or transaction remains open while awaiting ACK.
One dedicated OS thread isolates driver I/O, codecs, and mounted-file reads;
the existing shared encoder/checkpoint worker is separate.

This is a bounded adapter experiment, **not full P0 product qualification**.
Sources must retain selected values through replay, keep cursor and join/filter
values immutable, and provide commit-visible cursor ordering. A stable unique
`(timestamp, integer)` pair is an operator contract, not a required physical
UNIQUE constraint. Runtime duplicate detection cannot prove global uniqueness
or detect duplicates hidden behind a page boundary. Only one active poller per
logical source range is permitted across namespaces.

## Configuration

The complete native pipeline is
[`postgresql-otlp.yaml`](../../../../../configs/postgresql-otlp.yaml).
All configuration objects reject unknown fields.

Required fields are `source_id`, `connection`, `query`, and `checkpoint`.
`source_id` is 1-128 UTF-8 bytes without controls. `connection` requires one
DNS/IP host, database, password file, explicit TLS CA file, and exactly one of
`username` or `username_file`. Port defaults to 5432. Database and username
are 1-63 UTF-8 bytes; colons in usernames are permitted. No URI, host list,
interactive authentication, inline password, user options, insecure TLS mode,
or native trust-store fallback is accepted.

Credential/CA paths must be absolute after environment substitution and contain
no parent components. Atomic symlink-based Secret volume updates are allowed.
The platform must resolve its authorized external secret reference and mount
current material; this adapter is not a Key Vault/Kubernetes secret resolver.
Each connection reads the current password and CA, and checks that the current
username still matches the fingerprinted principal. Principal changes require
reviewed restart/reset. Password rotation alone does not change identity.
CR/LF suffixes are removed from credentials; spaces are preserved.

File reads use one opened handle with a limit-plus-one read, rather than the
shared TLS helper's metadata-size check followed by an unbounded read.
Username reads are bounded to 256 bytes, passwords to 16,384, CA bundles to
4 MiB and 64 certificates. Secret scratch buffers use zeroizing ownership.
Driver-internal copies are not claimed universally zeroized.

`query` requires SQL, ordered `result_schema`, and composite `watermark`.
Every schema entry requires `name`, canonical `source_type`, `nullable`, and
exact native `type_modifier`. Cursor columns must be projected once, non-null,
and backed by timestamp/timestamptz and int2/int4/int8 source columns.
The timestamp initial value is an explicit UTC `...Z` lower bound, not an
activation-time default. UTC is the only supported timezone.
Logical binds default to `last_timestamp` and `last_tie_breaker`; custom names
must be distinct `[a-z_][a-z0-9_]{0,63}` identifiers.

Interval defaults to 60s, allows 60s through 24h, and must resolve to whole
seconds (`90s` and `1.5m` work). `output.timestamp_column` is optional; omission
uses shared observed-time fallback. No additional output options are accepted.

Checkpoint defaults are `on_nack: rewind`, `on_permanent_nack: pause`,
`nack_backoff: 1s`, and `max_consecutive_failures: 5`. Permanent rejection
can instead use `retry`. The failure counter concerns durable writes.
Original SQL, bind names, native schema, connection principal, output mapping,
and explicit lower bound enter the deterministic `postgresql/v2` fingerprint.
Password material, interval, and rejection policy do not.

## SQL support

Named parameters are compiled token-by-token to native `$1` (timestamp) and
`$2` (integer), independent of occurrence order. Repeated timestamps share one
parameter. Native positional authoring is also accepted; styles cannot mix.
Strings, comments, Unicode, CRLF, and other bytes are preserved. No values are
interpolated and no LIMIT, cast, wrapper, or projection rewrite is executed.

The complete AST must be one SELECT with explicit qualified column projections,
optional unique output aliases, and one schema-qualified persistent ordinary
driving table with an alias. Up to three INNER JOINs are supported when equality
conjunctions cover a proven immediate, valid, ready, nonpartial,
nonexpression unique key on each joined side (INCLUDE attributes do not count).
Joins and filters must use compatible built-in source types. Views, RLS,
temporary/unlogged/foreign/partitioned tables and system schemas are rejected.

WHERE must be the complete keyset
`(ts > $1) OR (ts = $1 AND id > $2)`, or `(ts, id) > ($1, $2)`,
optionally ANDed with static direct-column/literal comparisons and NULL tests.
ORDER BY must be exactly the two direct driving cursor columns ascending.
No arbitrary OR, functions, casts, stars, computed expressions, subqueries,
WITH, DISTINCT, INTO, grouping, windows, locks, set operators, LIMIT/OFFSET/FETCH,
table functions, extra parameters, or trailing statement separators are allowed.
Catalog checks precede user SQL preparation and repeat within every page's
read-only READ COMMITTED transaction, including empty polls. Drift fails closed.

## Values and bounds

| Source types | Shared output policy |
| --- | --- |
| bool, int2/int4/int8 | Exact Bool/Int64 |
| numeric | Exact decimal string, including dscale and trailing zeros |
| float4/float8 | Finite Float64; exact float4 widening |
| text/varchar/bpchar | UTF-8 string with source-returned padding |
| bytea | OTLP BytesValue, not base64 text |
| timestamp/timestamptz | Six fractional digits; civil UTC / instant with Z |
| date, uuid | AD ISO date / lowercase canonical UUID strings |
| json/jsonb | Returned UTF-8 text and numeric lexemes; JSONB version 1 |
| interval | `months=<i32>;days=<i32>;microseconds=<i64>` |
| SQL NULL | Distinct Null, never an empty string or zero |

Nonfinite numerics/floats, invalid UTF-8, infinity timestamps, lossy cursor
precision, native tie overflow, and unrepresentable event nanoseconds fail.
Domains, arrays, ranges, composites, enums, extension types, OIDs/LOB handles,
TIME/TIMETZ, money, bit, XML, network, and geometric types are unsupported.
There is no generic stringification or destination dynamic fallback.

Fixed limits: 16 KiB SQL, 4,096 tokens, nesting 16, parser recursion 64,
128 output columns, 63-byte identifiers, 30s operation budget, 10s connect,
300-row portal fetches, 1,000-row pages, separate 8 MiB normalized and emitted
budgets, and 32 pages/10s catch-up admission. One candidate row may be decoded
outside the returned prefix; a first row that cannot fit is an error.
NUMERIC output is at most 16,384 characters.

The post-TLS plaintext guard rejects backend frame lengths outside 4..1 MiB
before exposing headers to the native decoder, reads at most 8 KiB and one frame
at a time, checks truncated frames, and preserves TLS channel binding.
Startup ParameterStatus count is at most 64; notices are suppressed and bounded
to 128 per operation. ParameterStatus after startup, idle notices, and
notifications terminate the session.
These logical bounds are not an RSS guarantee.

## Telemetry and runbook

The shared `DatabaseReceiverMetrics` and shared redacted diagnostic events are
used; emitted records carry `db.system.name=postgresql`. Native errors never
enter a diagnostic source chain. Server notices are not logged.
Do not install a log-facade bridge that publishes `tokio_postgres` targets:
the native driver has SQL-bearing debug logs. An embedding host must hard-filter
those targets even under runtime logging overrides. Console exporter output is
an intentional data sink, not a redaction surface.

On availability errors the shared controller retries only after confirmed local
cleanup. Cancellation uses a generation flag even while disconnected, requests
server cancellation with the verifying connector, observes terminal operation
and rollback, and disposes the session. A cancel-send result alone is not proof
of stop. Unconfirmed cleanup is terminal and shutdown cannot report success:
source ownership must remain quarantined until process restart/supervisor repair.
Missing worker-exit acknowledgments and failed thread joins remain terminal on
every subsequent shutdown attempt.

Never delete checkpoints automatically to recover an incompatibility. Review
the source, stop the poller, then deliberately reset state or select a new
`source_id`. Source replacement/restore requires the same explicit action;
logical schema alone cannot identify a new table incarnation after restart.

Full-product gaps remain: automatic go-forward activation, keyless timestamp
ties, arbitrary late-visible commits, exact interval/skip-busy scheduling,
uncapped logical runs, destination dynamic fallback, platform job/transform APIs,
and externally resolved secret references. Shared cadence is completion plus
interval with its existing 1/2/4/8/16/30s failure backoff.

## Validation

Run from `rust/otap-dataflow`:

```powershell
cargo check -p otel-arrow-dfe-contrib-nodes --no-default-features `
  --features postgresql-receiver
cargo test -p otel-arrow-dfe-contrib-nodes --no-default-features `
  --features postgresql-receiver receivers::postgresql_receiver --lib
cargo clippy -p otel-arrow-dfe-contrib-nodes --no-default-features `
  --features postgresql-receiver --all-targets -- -D warnings
cargo test -p otel-arrow-dfe-config bundled_configs_parse_as_engine_configs
cargo test -p otel-arrow-dfe-scraper
cargo build --bin df_engine --no-default-features `
  --features postgresql-receiver,otlp,crypto-ring
cargo xtask component-inventory
```

Unit tests exercise exact parser/codec/frame/credential boundaries and persistent
cleanup failures. Loopback TLS tests exercise the production connector's chain
and hostname verification and its post-TLS frame guard. These are not a mock
database pretending to establish server correctness. The bundled-config test
uses scoped temporary `PG_*` path substitutions, restores the environment, and
does not require deployment credentials or introduce production defaults.
Live qualification needs a
real PostgreSQL 15 server with a least-privilege read-only role, trusted matching
certificate, mounted credentials, stable fixture rows, and an accepting OTLP
capture endpoint. Run the example pipeline through the built `df_engine`, not
only a driver probe. Count exact source IDs and multiplicities, inspect OTLP
types, then exercise restart/replay, NACK, schema drift, TLS negatives, secret
rotation and cancellation faults. Do not claim these pass from unit coverage.

The executable live case is `examples/postgresql_e2e.rs`, not an ignored test.
Ask the provisioning harness to load `fixture.sql` in an empty fixture database
and grant SELECT to its existing receiver role. Set the four `PG_*` environment
variables used by the pipeline. The runner creates an isolated checkpoint,
binds a loopback ephemeral OTLP capture, launches the actual engine, withholds
ACK, crashes that child only, verifies exact replay, and then checks all 2,305
accepted IDs, multiplicities, timestamps, decimal scale, and JSON integer text:

```powershell
cargo run -p otel-arrow-dfe-contrib-nodes --no-default-features `
  --features postgresql-receiver --example postgresql_e2e -- `
  target\debug\df_engine.exe configs\postgresql-otlp.yaml
```

Missing fixture/server/credentials cause a hard failure, not a skipped pass.
This case does not provision users or alter server configuration. It does not
replace the remaining TLS, metadata-drift, rotation, or soak qualification.

A 60-minute mixed-width soak under the reference's 512 MiB process envelope
requires separate measurement of both workers, TLS/driver allocations,
downstream delay, throughput, source load, and recovery. No performance or
production approval follows from compilation.
