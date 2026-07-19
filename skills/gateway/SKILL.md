---
name: gateway
description: Gateway and Config Management skill for UniversalRuntime Phase 1.
---

# Gateway and Config Management

Implement compatibility API, native API, immutable config revisions, outbox transaction and SSE coordination.

## Acceptance
- Gateway never imports app code
- config validate/create/activate
- exact compatibility envelopes
- native typed errors

## Engineering constraints

Follow root `AGENTS.md`. Update contracts and tests in the same change. Never claim completion before commands and outputs are recorded.
