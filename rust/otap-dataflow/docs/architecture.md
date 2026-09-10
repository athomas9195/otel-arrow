# Architecture

TBD

## Processor architecture

Processor roles, behavior classes, and exclusive-router guarantees are
documented in [Processors](processors.md).

## Memory resource management

The relationships among process memory protection, receiver throttling,
allocator attribution, retained-work accounting, durable buffering, and future
scoped policies are documented in
[Memory Resource Management](memory-resource-management.md).

## Admin UI architecture

The embedded admin web UI architecture is documented in:

- [Admin UI Architecture](admin/architecture.md)

## Load-balancing considerations

See [Load Balancing: Challenges & Solutions](load-balancing.md).

## Database polling receivers

The proposed shared behavioral contract and initial near-source deployment
profile are documented in
[Database Polling Receiver Contract](database-polling-receiver-contract.md).

The proposed crate boundaries, link-time component composition, and
customer-side agent distribution are demonstrated in the
[Agent-Based Database Receiver Architecture Skeleton](database-receiver-agent-architecture/README.md).
