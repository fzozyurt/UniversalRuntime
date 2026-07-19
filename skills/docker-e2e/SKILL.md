---
name: docker-e2e
description: Docker and E2E skill for UniversalRuntime Phase 1.
---

# Docker and E2E

Build one image supporting standalone/gateway/dispatcher/worker/projector modes. Provide Compose with HAProxy, PostgreSQL and Kafka.

## Acceptance
- official SDK E2E
- priority test
- worker kill/retry
- Gateway load balancing
- FastAPI and A2A smoke

## Engineering constraints

Follow root `AGENTS.md`. Update contracts and tests in the same change. Never claim completion before commands and outputs are recorded.
