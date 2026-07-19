---
name: grpc-worker
description: gRPC Worker Protocol skill for UniversalRuntime Phase 1.
---

# gRPC Worker Protocol

Generate protobuf clients, implement register/work/drain and standard gRPC health.

## Acceptance
- Value handles scalar/list/object/null
- capability/config hash handshake
- bounded concurrency slots
- deterministic graceful shutdown

## Engineering constraints

Follow root `AGENTS.md`. Update contracts and tests in the same change. Never claim completion before commands and outputs are recorded.
