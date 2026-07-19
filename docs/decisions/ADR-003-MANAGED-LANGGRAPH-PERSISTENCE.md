# ADR-003: Use upstream LangGraph persistence implementations

## Status

Accepted for Phase 1.

## Decision

Use upstream InMemory saver/store locally and upstream AsyncPostgresSaver/AsyncPostgresStore in production. Do not redesign native checkpoint payloads or tables.

## Consequences

- Full LangGraph semantics are retained.
- Native setup/migrations run under an advisory lock.
- Application state schemas are isolated from FastAPI application schemas.
- Schema targeting follows the verified upstream capability; a dedicated role and fixed search path is the fallback.
