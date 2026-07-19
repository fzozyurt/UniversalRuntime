# Phase 1 Architecture

## 1. Context

Phase 1 deliberately optimizes for a real LangGraph-compatible MVP while preserving a universal adapter core.

```text
Clients / langgraph_sdk / Chat UI / A2A Client
                         |
                         v
                    HTTP Gateway
       LangGraph Compatibility + Native API + Config API
                         |
                PostgreSQL transaction
                 Run + Outbox command
                         |
                         v
                Priority Queue Boundary
         Local: asyncio | Production: Kafka topics
                         |
                         v
                     Dispatcher
          thread ordering + lease + worker selection
                         |
                         v gRPC
                       Worker
          LangGraph adapter + FastAPI adapter + SDK
              |                         |
              v                         v
  Postgres Checkpointer/Store      App DB / HTTP tools
                         |
                         v
             Runtime events / lifecycle events
                         |
               Event Gateway / Projector
                         |
            SSE replay / DB / OpenSearch sinks
```

## 2. Deployable modes

### `standalone`

Gateway, dispatcher and worker execute in one process for local development. Uses InMemory infrastructure unless explicitly overridden.

### `gateway`

Only public APIs, config management and stream delivery. It never imports user application code.

### `worker`

Loads user application code, registers over gRPC, receives leases, executes and emits events.

### `dispatcher`

Consumes run topics, applies weighted priority and routes work to workers.

### `projector`

Consumes lifecycle/runtime events and writes structured projections to PostgreSQL/OpenSearch/custom sinks.

## 3. Control and data boundaries

### Gateway owns

- public compatibility/native APIs,
- application and config revisions,
- thread/run identifiers,
- request validation,
- run/outbox transaction,
- SSE delivery/replay coordination.

### Worker owns

- user code loading,
- framework adapter execution,
- managed checkpointer/store binding,
- FastAPI custom application process,
- runtime event conversion,
- execution telemetry.

### Dispatcher owns

- queue consumption,
- priority fairness,
- thread lease and ordering,
- worker concurrency slots,
- retry and dead-letter routing.

## 4. Config distribution

1. Gateway stores immutable config revision.
2. A deployment activates one revision.
3. Worker registration includes application/revision/deployment identifiers.
4. Gateway/dispatcher sends the resolved config hash and protected execution defaults.
5. Worker refuses a lease if config hash/revision does not match its loaded image/config.

## 5. State

- Platform metadata: PostgreSQL.
- LangGraph checkpoint/store: native upstream PostgreSQL saver/store.
- Local state: upstream InMemory saver/store.
- Large event/tool content: artifact store in later slice; Phase 1 may retain bounded JSON with size limits.
- Runtime events: ordered by run sequence; persisted in batches, not one database row per token.

## 6. Scaling

- Gateway replicas are stateless except stream connections and use shared PostgreSQL/event transport.
- Dispatcher replicas coordinate using Kafka consumer groups and DB leases.
- Workers advertise capacity.
- `maxConcurrency` is a capacity count, not a promise of CPU resources.
- Same-thread commands are partitioned consistently and protected by a DB uniqueness/lease constraint.

## 7. Priority policy

Separate Kafka topics provide real priority classes. The dispatcher uses a weighted cycle and age promotion, so interactive requests stay fast without permanently starving batch work.

## 8. Failure behavior

- Gateway transaction commits before Kafka publish via outbox.
- Duplicate commands are rejected by inbox/idempotency keys.
- Worker heartbeat loss expires attempt lease.
- Retry creates a new `attempt_id`, never a new `run_id`.
- A stream disconnect does not automatically cancel a run unless the requested disconnect policy says so.
