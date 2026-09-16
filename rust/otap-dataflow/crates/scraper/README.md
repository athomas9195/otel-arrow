# Shared scraper infrastructure

This crate is the shared, database-neutral home for OTAP receiver scraping.
The shared controller polls through database-neutral driver contracts, maps rows
to OTLP, and handles backpressure and ACK/NACK feedback. It admits one pending
page and advances its cursor only after a matching ACK and a successful write
through the checkpoint contract. A NACK retains the committed cursor for replay.
Delivery is at least once, not exactly once, only when the source exposes rows
in commit/cursor order and retains immutable rows throughout delivery and replay.
Append-only data alone does not prevent late commits behind the watermark.

`CheckpointStore` supplies revisioned, checksummed, atomically installed state.
`SourceLease` combines a process-local registry and advisory filesystem locking.
The controller retains its ownership guard until active work ends. A missed
worker stop deadline or abnormal task drop quarantines ownership until process
exit; a supervisor must hard-stop the process if native teardown hangs. Separate
checkpoint stores can still collect the same logical source: this is not a
distributed source registry.

The concrete filesystem backend and a test-only in-memory backend both implement
the same contracts. No vendor driver or receiver is registered by this crate.

## Dependency boundary

- Database receiver modules in `contrib-nodes` may depend on shared scraper
  contracts and their own optional database drivers.
- Shared scraper code must not depend on a vendor receiver or database driver.
- The executable composes registered components and owns application startup.
- Helm charts, container images, and installation scripts are deployment assets,
  not dependencies of the shared runtime.
- Runtime integration reuses the existing engine, telemetry, and pdata APIs.
  Local async contracts preserve the engine's thread-per-core model.

## Follow-on changes

Add the optional Oracle adapter. Whole-poll/normal ACK deadlines,
memory-pressure admission, and late-visible-row recovery remain follow-up work.

Database authentication through extension capabilities is a separate follow-up,
not a new credential mechanism introduced by this skeleton.

See [the database receiver RFC](https://github.com/open-telemetry/otel-arrow/issues/3918)
for the broader design. This crate scaffold does not claim that the full RFC is
implemented.
