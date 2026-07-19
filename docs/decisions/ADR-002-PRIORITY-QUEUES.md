# ADR-002: Separate Kafka topics for priority classes

## Status

Accepted for Phase 1.

## Decision

Use separate interactive, normal and batch run topics. Dispatcher reads them using weighted fair scheduling and age promotion.

## Rationale

Kafka preserves order inside a partition but does not reorder an already queued batch record when a chat request arrives. Separate topics provide predictable low latency.

## Consequences

- More topics and consumer logic.
- Same-thread ordering remains protected by DB lease and partition key.
- Batch starvation must be prevented by weights and aging.
