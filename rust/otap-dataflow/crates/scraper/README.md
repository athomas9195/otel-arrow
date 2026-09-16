# Shared scraper infrastructure

This crate is the shared, database-neutral home for OTAP receiver scraping.
This layer adds database-neutral configuration, query, value, cursor, page, and
driver contracts, plus checkpoint and source-ownership interfaces. It does not
implement polling, encode OTLP, persist state,
open database connections, register a receiver, or enable new behavior in
`df_engine`.

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

The polling controller depends on `CheckpointBackend` and `SourceOwnership`,
not a concrete filesystem implementation. This lets the next change introduce
polling, delivery and OTLP mapping with test-only fake persistence. A subsequent
change supplies durable filesystem checkpoints and source leases, followed by
the optional Oracle adapter.

Database authentication through extension capabilities is a separate follow-up,
not a new credential mechanism introduced by this skeleton.

See [the database receiver RFC](https://github.com/open-telemetry/otel-arrow/issues/3918)
for the broader design. This crate scaffold does not claim that the full RFC is
implemented.
