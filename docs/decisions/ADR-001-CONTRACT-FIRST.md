# ADR-001: Contract-first runtime core

## Status

Accepted for Phase 1.

## Decision

Public compatibility, native API, protobuf, config, Kafka event and database naming contracts are versioned before implementation. Framework code is isolated behind adapters.

## Consequences

- Adding a framework does not change Gateway/run domain semantics.
- Compatibility endpoints can evolve independently from native APIs.
- Protobuf changes require backward-compatibility review.
- Unsupported framework capabilities remain explicit instead of being faked.
