---
name: streaming-events
description: Streaming and Event Projection skill for UniversalRuntime Phase 1.
---

# Streaming and Event Projection

Convert native framework events to canonical runtime events and back to LangGraph SSE. Publish lifecycle events for DB/OpenSearch projection.

## Acceptance
- monotonic run sequence
- namespace preserved
- token events batched for persistence
- replay cursor tested

## Engineering constraints

Follow root `AGENTS.md`. Update contracts and tests in the same change. Never claim completion before commands and outputs are recorded.
