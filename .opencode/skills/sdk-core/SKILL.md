---
name: sdk-core
description: SDK Core skill for UniversalRuntime Phase 1.
---

# SDK Core

Implement one modular Python SDK with domain, application, ports, adapters, configuration, telemetry and bootstrap modules.

## Required ports
- runtime adapter
- config repository
- run/thread repositories
- priority queue
- event publisher/replay
- worker registry

## Acceptance
- local composition root uses only InMemory adapters
- no unbounded tasks
- typed errors and DTO mapping

## Engineering constraints

Follow root `AGENTS.md`. Update contracts and tests in the same change. Never claim completion before commands and outputs are recorded.
