---
name: postgres
description: PostgreSQL Persistence skill for UniversalRuntime Phase 1.
---

# PostgreSQL Persistence

Implement platform models/migrations, schema naming, advisory locks, outbox/inbox, event batches and managed LangGraph saver/store setup.

## Acceptance
- state survives restart
- app/state schemas isolated
- concurrent migration attempt runs once
- one active run per thread constraint

## Engineering constraints

Follow root `AGENTS.md`. Update contracts and tests in the same change. Never claim completion before commands and outputs are recorded.
