# Gateway Service

Responsibilities:

- LangGraph compatibility API,
- native management/config API,
- IDs and idempotency,
- PostgreSQL run + outbox transaction,
- SSE delivery and replay coordination,
- A2A Agent Card generation.

The Gateway must not import or execute user graph code and must not create Kubernetes resources directly.


The local composition exposes `universal_runtime.services.gateway.app:create_app`; compatibility routes keep LangGraph response shapes, while `/api/v1/applications/*/config` returns the native `{data, meta}` response and typed errors.
