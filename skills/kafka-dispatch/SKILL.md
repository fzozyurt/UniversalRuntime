---
name: kafka-dispatch
description: Kafka Priority Dispatch skill for UniversalRuntime Phase 1.
---

# Kafka Priority Dispatch

Implement interactive/normal/batch topics, weighted fairness, aging, partition keys, idempotency and delayed acknowledgements.

## Acceptance
- interactive wins over queued batch
- batch eventually runs
- same-thread ordering
- poison message dead letter

## Engineering constraints

Follow root `AGENTS.md`. Update contracts and tests in the same change. Never claim completion before commands and outputs are recorded.
