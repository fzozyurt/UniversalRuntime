# Integration tests

## PostgreSQL

- platform Alembic migration lock,
- LangGraph saver/store `setup()` under lock,
- application schema isolation,
- worker restart state continuity,
- outbox/inbox idempotency,
- one active run per thread.

## Kafka

- separate priority topics,
- weighted dispatcher selection,
- age promotion,
- partition key ordering,
- delayed commit until DB transition,
- dead-letter behavior.

## gRPC

- registration/capability handshake,
- max concurrency slots,
- lease/heartbeat/drain,
- arbitrary JSON Value serialization,
- cancellation and completion.
