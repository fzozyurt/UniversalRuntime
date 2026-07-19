# Gateway Service

Responsibilities:

- LangGraph compatibility API,
- native management/config API,
- IDs and idempotency,
- PostgreSQL run + outbox transaction,
- SSE delivery and replay coordination,
- A2A Agent Card generation.

The Gateway must not import or execute user graph code and must not create Kubernetes resources directly.
