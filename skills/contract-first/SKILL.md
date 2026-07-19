---
name: contract-first
description: Contract First skill for UniversalRuntime Phase 1.
---

# Contract First

Own OpenAPI, Protobuf, config schema, event schema, Kafka and database contracts.

## Rules
- implementation follows contracts
- compatibility and native APIs are separate
- protobuf changes are additive
- validate contracts in CI

## Engineering constraints

Follow root `AGENTS.md`. Update contracts and tests in the same change. Never claim completion before commands and outputs are recorded.
