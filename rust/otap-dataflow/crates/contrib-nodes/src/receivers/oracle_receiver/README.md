# Oracle OCI Receiver

This experimental receiver polls Oracle through the OCI-backed Rust `oracle`
crate and Oracle Instant Client. It uses the shared database receiver runtime:

- `receivers/database/config.rs` validates database-neutral limits, scheduling,
  retry, output, and named-query configuration.
- `receivers/database/receiver.rs` owns the OTAP lifecycle, independent query
  jobs, admission, downstream sends, and shutdown.
- `receivers/database/scheduler.rs` provides Delay scheduling, startup jitter,
  non-overlap, retries, and receiver-wide concurrency and byte reservations.
- `receivers/database/driver.rs` defines the database adapter contract.
- `receivers/database/query.rs` compiles SQL and typed logical bind values
  without inserting values into SQL text.
- `receivers/database/row.rs` normalizes metadata and typed values into bounded
  row batches.
- `receivers/database/otlp.rs` maps one row to one typed OTLP log record.
- `receivers/database/metrics.rs` reports stable, low-cardinality database and
  per-query metrics.
- `receivers/oracle_receiver/adapter.rs` implements the shared contract.
- `receivers/oracle_receiver/worker.rs` owns each synchronous OCI connection on
  a dedicated blocking worker.

Checkpoint storage, watermark query modes, and ACK/NACK coordination are not
implemented here. Those delivery-position concerns are intentionally separate
from snapshot polling.

## Behavior

Each named query:

- starts independently after optional startup jitter;
- waits its configured interval after a poll completes;
- never overlaps another execution of itself;
- uses server call timeouts and Oracle connection-break cancellation;
- binds scalar parameters separately from SQL text;
- validates unique, case-insensitive result-column names;
- converts supported Oracle values to database-neutral typed values;
- enforces hard row, batch-row, batch-byte, and page-byte limits;
- waits for downstream admission before scheduling its next interval; and
- optionally retries transient connection and timeout failures with bounded
  exponential backoff.

Receiver-wide concurrency, in-flight bytes, and OTAP process memory pressure
can all stop admission of new polls. One dedicated Oracle connection worker is
used per active session, and pool limits bound the worker count.

The default OTLP body is a structured key-value object. Selected columns can
also become typed attributes. Every log includes the configured query name,
`db.system.name=oracle`, receiver observation time, and an optional source event
time. Resource attributes are limited to operator-approved scalar values.

Use a database account with only the read permissions required by the queries.
SQL must start with `SELECT` or `WITH`.

## Configuration

```yaml
type: urn:otel:receiver:oracle
config:
  database:
    connect_string: //localhost:1521/FREEPDB1
    username: PDBADMIN
    password_env: ORACLE_PWD
    pool:
      min_connections: 1
      max_connections: 2
      max_connection_lifetime: 1h
    tls:
      mode: disabled
  limits:
    max_concurrent_queries: 2
    max_batch_rows: 1000
    max_batch_bytes: 8 MiB
    max_in_flight_bytes: 16 MiB
    fetch_size: 100
  scheduling:
    startup_jitter_max: 5s
  retry:
    enabled: true
    initial_backoff: 1s
    multiplier: 2
  resource_attributes:
    service.name: oracle-database
  queries:
    current_time:
      sql: SELECT SYSDATE AS CURRENT_TIME FROM DUAL
      interval: 30s
      timeout: 10s
      pagination:
        max_rows: 100
      output:
        attributes:
          CURRENT_TIME: database.current_time
        timestamp:
          column: CURRENT_TIME
        oversize_policy: error
      error_policy: stop_receiver
```

The password is read from the environment variable named by `password_env` on
each new connection. It is not stored in the pipeline configuration. Connection
lifetime rotation allows reconnects to load rotated credentials and TLS state.

`database.tls.mode` supports:

- `disabled` for a non-TCPS connection;
- `verify_full` for a `tcps://` Easy Connect string plus an Oracle wallet and
  server identity verification; and
- `insecure` for explicit TCPS encryption without server identity
  verification.

The receiver requires a single-core pipeline so multiple pipeline cores do not
independently poll and emit the same snapshot rows.

## Local Oracle Database Free

Make Oracle Instant Client available through `PATH`, set the password, and run
the sample pipeline:

```powershell
$env:PATH = "C:\path\to\instantclient_23_26;$env:PATH"
$env:ORACLE_PWD = "your-local-password"

cd rust\otap-dataflow
cargo run --features oracle-receiver -- `
  --config configs\oracle-oci-console.yaml `
  --num-cores 1
```

## Tests

Unit tests cover shared configuration, scheduling, admission, row bounds, OTLP
mapping, Oracle configuration, and error classification without requiring an
Oracle server:

```powershell
cd rust\otap-dataflow
cargo test -p otap-df-contrib-nodes --features oracle-receiver --lib
```

To run the opt-in live test:

```powershell
$env:PATH = "C:\path\to\instantclient_23_26;$env:PATH"
$env:OTAP_ORACLE_RECEIVER_E2E = "1"
$env:ORACLE_USERNAME = "PDBADMIN"
$env:ORACLE_PWD = "your-local-password"
$env:ORACLE_CONNECT_STRING = "//localhost:1521/FREEPDB1"

cargo test -p otap-df-contrib-nodes --features oracle-receiver `
  oracle_receiver_emits_rows_when_configured -- --nocapture
```
