# Shared scraper infrastructure

This crate is the shared, database-neutral home for OTAP receiver scraping.
The shared controller polls through database-neutral driver contracts, maps rows
to OTLP, and handles backpressure and ACK/NACK feedback. It admits one pending
page and advances its cursor only after a matching ACK and a successful write
through the checkpoint contract. A NACK retains the committed cursor for replay.
Delivery is at least once, not exactly once.

This layer has no filesystem checkpoint backend, source-lock implementation,
vendor driver, or receiver registration. A test-only in-memory backend exercises
the production loop independently of persistence.

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

Add durable filesystem checkpoints and source leases implementing
`CheckpointBackend` and `SourceOwnership`, followed by the optional Oracle
adapter. Whole-poll/normal ACK deadlines, memory-pressure admission, tighter
cleanup bounds, and late-visible-row policy remain follow-up work.

Database authentication through extension capabilities is a separate follow-up,
not a new credential mechanism introduced by this skeleton.

See [the database receiver RFC](https://github.com/open-telemetry/otel-arrow/issues/3918)
for the broader design. This crate scaffold does not claim that the full RFC is
implemented.
