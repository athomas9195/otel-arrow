# Oracle composite watermark receiver

The opt-in `oracle-receiver` feature registers
`urn:otel:receiver:oracle`. The receiver polls a customer-authored, read-only
`SELECT` after a durable composite watermark, emits one typed OTLP `LogRecord`
per row, and advances its checkpoint only after a downstream acknowledgement.

## Delivery guarantees

Delivery is **at least once only for sources meeting the requirements below**.
A crash, a negative acknowledgement, or a drain
that ends before feedback arrives can re-emit rows, but an unacknowledged row is
never intentionally skipped.

Each source has at most one page awaiting downstream feedback. An ACK commits
that page's cursor; a NACK retains the durable cursor and replays the page.

The source must expose commits in increasing composite-cursor order: after a
cursor is acknowledged, no transaction may later make an older cursor visible.
Append-only tables alone do not ensure this. A timestamp assigned before commit,
late commits, backdated inserts, or updates that move a cursor backwards can
cause rows to be missed and are unsupported.

Rows and their cursor values must remain unchanged and retained until delivery
is acknowledged and through any outage/replay window. A NACK re-executes SQL; it
does not retain an immutable copy of the original page. Deleted or changed rows
cannot be faithfully replayed. Choose retention longer than the maximum expected
outage plus downstream retry duration. This receiver is not CDC and cannot
enforce source commit order or retention.

## Supported watermark mode

Only `watermark.mode: composite` is implemented:

- an ordered timestamp column of an Oracle `DATE` or `TIMESTAMP`-family type
- a non-null `int64` tie-breaker that is unique within each timestamp group
- UTC semantics; `watermark.timestamp.timezone` must be `UTC`

Both cursor columns must be declared `NOT NULL`. Tie-breaker metadata must be
`Int64` or `NUMBER(p,0)` with `1 <= p <= 18`, whose complete range fits signed
64-bit storage. Unsigned, unconstrained, and wider NUMBER cursor columns are
rejected before polling; non-cursor NUMBER columns remain precision-preserving
decimal strings.

`scalar` and repeating `snapshot` modes are rejected as unsupported and are
deferred to follow-up work.

## Required query shape

You supply the complete SQL. The receiver validates, before connecting, that the
statement:

- is a single `SELECT` without SQL comments or statement separators
- has no subquery or set operation, and uses exactly the predicate
  `timestamp > :timestamp OR (timestamp = :timestamp AND id > :id)`,
  optionally enclosed in one additional pair of parentheses; arbitrary
  additional predicates are not supported in this initial conservative validator
- references both configured named binds as real bind markers -- a bind name
  appearing only inside a string literal, or only as a prefix of a longer bind,
  does not count
- ends with the outer ordering
  `ORDER BY <timestamp_column> ASC, <tie_breaker_column> ASC`; an ordering
  nested inside a subquery does not satisfy this

Cursor values are bound through Oracle named parameters and are never
interpolated into SQL text. Live result metadata is then checked so both cursor
columns exist with supported, deterministic types.

## Bounds

Operational bounds are explicit:

- `query.max_rows_per_poll` -- hard row ceiling for one poll
- `query.fetch_size` -- target rows per Oracle driver fetch, capped by the row
  and byte limits
- `query.max_batch_bytes` -- **exact** serialized OTLP payload ceiling
- `query.max_normalized_bytes` -- independent retained normalized row storage
  ceiling, including value capacities and cursor/page storage

The receiver emits the largest non-empty row prefix that fits
`max_batch_bytes`, and the committed candidate is always the cursor of the last
row actually emitted. Normalized rows have their own independent ceiling.
Rows beyond a ceiling are returned by the next poll rather than dropped. If a
single first row exceeds the byte ceiling, the poll fails explicitly instead
of skipping it.

Neither ceiling is a process-RSS limit. Native Oracle fetch buffers, column
metadata, allocator overhead, the OTLP record representation and the serialized
buffer can coexist. Values move into OTLP records rather than cloning strings
and bytes; binary columns remain OTLP bytes. Encoding runs on a blocking worker,
with one bounded page per receiver and no additional queued polls.

The `large_owned_page_keeps_local_runtime_responsive` regression encodes 10,000
rows with 4 KiB strings while a local timer continues running. A Windows debug
test-process measurement sampled a peak working set of 92.7 MiB for that case.
This is an illustrative encoding-only measurement, not an RSS guarantee or an
Oracle end-to-end memory benchmark; OCI buffers are not included.

## Checkpoints and replay

Checkpoints are revisioned files under `checkpoint.directory`, keyed by pipeline
group, pipeline, receiver name, and `source_id`. Each file records a schema
version, revision, source identity, configuration fingerprint, composite cursor,
and checksum. Writes use a same-directory temporary file, `fsync`, and an atomic
rename, and the two newest revisions are retained.

Reads fail closed on corruption, an unsupported version, or a revision, source,
or fingerprint mismatch, so a receiver never resumes from an unrelated position.
The configuration fingerprint covers semantic fields only, so rotating a mounted
credential does not invalidate durable state.

`checkpoint.on_nack` supports only `rewind`. A negative acknowledgement retains
the committed cursor and replays the same page after the fixed
`checkpoint.nack_backoff`. Stale or duplicate feedback is ignored. Reaching
`checkpoint.max_consecutive_failures` durable-write failures terminates the
receiver with a checkpoint error without advancing in-memory state.

A filesystem lease keyed by the checkpoint identity prevents two receiver
processes sharing the state directory from advancing the same checkpoint.
Deployments must still ensure one replica owns each checkpoint source when the
state directory is not shared.

This is checkpoint ownership, not database-source discovery. Two pipelines with
different pipeline/receiver names or checkpoint directories can poll the same
database query concurrently. Operators must enforce one owner per actual
database source across these identities and replicas. Renaming a receiver or
changing its state location does not transfer its checkpoint.

## Live configuration changes

Stop the existing pipeline before starting it with a changed configuration,
including interval-only changes. Live replacement starts the new receiver before
stopping the old one, so the old lease prevents replacement. Shared extension
authentication and live replacement are follow-ups, not supported capabilities
of this initial receiver (see upstream issues #4001 and #4049, and PR #4077).
Mounted credential files are the initial node-local mechanism; no shared auth
extension is introduced by this stack.

## Shutdown and native workers

One bounded dedicated worker owns each Oracle session, prepared statement,
connection setup, queries, and destruction. The driver is synchronous and OCI
cleanup can perform network I/O; no native session returns to the pipeline core.
Cancellation uses a separate blocking call to `break_execution` and checks a
cancel flag between fetches. Checkpoint I/O and encoding also run off-core.

Cancellation and worker join wait until the earlier of the active stop deadline
and five seconds. Checkpoint writes continue handling control messages and honor
an already-active drain deadline. If work cannot be joined, the receiver reports
a shutdown error and quarantines its lease until process exit rather than
allowing a replacement to overlap unfinished work. Restart the process to recover.
The service supervisor must enforce a hard process-stop timeout: a stuck native
thread may prevent Tokio runtime teardown from completing. Do not restart only
the pipeline or delete lock/generation files while that process is alive.

## Telemetry

The receiver registers the `receiver.database` metric set covering starts,
polls, query failures, batches, rows, encoded bytes, acknowledgements, negative
acknowledgements, replays, stale feedback, checkpoint commits, checkpoint
failures, checkpoint cleanup failures, cancellations, drains, and shutdowns.

## Running

Use `configs\oracle-oci-console.yaml` as the complete example. Credentials must
be regular UTF-8 files; keep their contents outside YAML and environment
variables. Run the receiver on a single pipeline core:

Install the matching-architecture Oracle Instant Client Basic (or Basic Light)
package following the [Oracle installation instructions][instant-client].
On Linux, install its documented OS prerequisites and configure the loader using
`ldconfig` or `LD_LIBRARY_PATH` before starting the engine. On Windows, install
the required Visual C++ runtime and add the Instant Client directory to `PATH`.
Set `connection.instant_client_dir` to that directory. All Oracle receivers in
one process must use the same directory.

```powershell
$env:PATH = "C:\oracle\instantclient;$env:PATH"
cargo run --no-default-features --features crypto-ring,oracle-receiver -- `
  --config configs\oracle-oci-console.yaml --num-cores 1
```

```sh
export LD_LIBRARY_PATH="/opt/oracle/instantclient:${LD_LIBRARY_PATH:-}"
cargo run --no-default-features --features crypto-ring,oracle-receiver -- \
  --config configs/oracle-oci-console.yaml --num-cores 1
```

These commands require an existing Oracle instance and mounted credential files.
There is no Docker Compose demo in this stack.

[instant-client]: https://www.oracle.com/database/technologies/instant-client/downloads.html

## Deterministic load generation

The `oracle_load_generator` example creates `OTAP_ORACLE_EVENTS` and inserts a
requested number of deterministic rows. `--collision-size` controls how many
consecutive rows share one timestamp, which verifies that the composite
timestamp plus tie-breaker cursor neither skips nor duplicates rows at page
boundaries. `--reset` recreates the table before loading it.
Existing IDs are left unchanged on repeat runs; this is not an update/upsert.

Run it directly against an existing Oracle instance using
`ORACLE_USERNAME`, `ORACLE_PWD`, and `ORACLE_CONNECT_STRING`:

```powershell
cargo run -p otel-arrow-dfe-contrib-nodes `
  --features oracle-receiver --example oracle_load_generator -- `
  --reset --rows 10000 --collision-size 100
```

For the opt-in live smoke test, set `OTAP_ORACLE_RECEIVER_E2E=1` plus
`ORACLE_CONNECT_STRING`, `ORACLE_INSTANT_CLIENT_DIR`,
`ORACLE_USERNAME_FILE`, and `ORACLE_PASSWORD_FILE`, then run:

```powershell
cargo test -p otel-arrow-dfe-contrib-nodes `
  --features oracle-receiver `
  emits_oracle_rows_when_live_test_is_enabled -- --nocapture
```
