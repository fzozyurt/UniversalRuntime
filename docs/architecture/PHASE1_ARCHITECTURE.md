# Phase 1 Architecture

## Context

UniversalRuntime keeps framework state behind adapters and uses application-owned Kafka topics for horizontal worker scaling.

```text
Clients / langgraph_sdk / Chat UI / A2A Client
                         |
                         v
                    HTTP Gateway
       LangGraph Compatibility + Native API + Config API
                         |
              create run and command
                         |
                         v
 rt.<environment>.<application_id>.runs.<priority>.v1
                         |
            shared application consumer group
                         |
              +----------+----------+
              |                     |
           Worker 1              Worker N
       application code       application code
       framework adapter      framework adapter
              |                     |
              +----------+----------+
                         |
       framework-owned persistence providers
                         |
       LangGraph Postgres Checkpointer / Store

Worker live events
        |
        v
rt.<environment>.<application_id>.execution.events.v1
        |
        +--> Gateway 1 live SSE queues
        +--> Gateway 2 live SSE queues
```

There is no Dispatcher or Event Projector deployment. Kafka consumer groups share work between Worker replicas. Framework history is not copied into a Runtime event journal.

## Deployable modes

### `standalone`

Runs the local composition with InMemory repositories and adapters.

### `gateway`

Owns public APIs, config management, run creation, live stream delivery and the internal worker-control gRPC service. It never imports user application code in production.

### `worker`

Loads application code and framework adapters, exposes worker gRPC control, registers with Gateway, waits for migration readiness, then consumes application Kafka topics.

### `api`

Runs custom FastAPI routes as a separate process. Gateway reverse-proxies to this deployment.

### `all`

Runs Gateway and Worker in one process while preserving the same gRPC registration, migration and Kafka contracts.

## Ownership boundaries

### Gateway

- LangGraph-compatible and native APIs
- application/config revisions
- execution identity resolution
- thread/run creation
- application-scoped Kafka command publication
- worker registry and heartbeats
- migration ownership coordination
- live SSE fan-out

### Worker

- user-code loading
- framework adapter execution
- framework persistence binding
- RuntimeEvent conversion
- Runtime-owned Alembic revision execution
- execution telemetry

### Application API

- custom HTTP handlers
- folder-based routers and DTOs
- application request/response schemas and examples

### Framework adapter

- checkpoint state
- state history
- interrupt/resume state
- framework-specific storage semantics

For LangGraph, `AsyncPostgresSaver` and `AsyncPostgresStore` are authoritative. Another adapter may bind a different persistence implementation without changing the Runtime control plane.

## Worker registration and migration

Registration is internal gRPC traffic.

```text
Worker starts gRPC server
        |
        v
Gateway WorkerControl.Register
        |
        +-- no app revisions -------------> ready
        +-- revision already installed ---> ready
        +-- this worker owns migration ----> WorkerControl.Migrate ---> ready
        +-- another worker owns migration -> wait for DB state ------> ready
```

Migration idempotency is scoped by:

```text
application_id + workspace_key + environment + app_version
```

The row stores owner worker, target revision, attempt number, status and error. Stale claims are recoverable. A Worker does not consume Kafka before registration returns `accepted=true`.

Applications provide only revision files. Runtime owns Alembic `env.py`, `script.py.mako`, connection lifecycle, schema selection, version-table placement and advisory locking.

## Kafka routing

```text
rt.<environment>.<application_id>.runs.short_queue.v1
rt.<environment>.<application_id>.runs.long_queue.v1
rt.<environment>.<application_id>.execution.events.v1
rt.<environment>.<application_id>.deadletter.v1
```

Gateway publishes from the run's `ExecutionIdentity.application_id`. Worker replicas for the same application share:

```text
rt.<environment>.<application_id>.workers.v1
```

Partition affinity is:

```text
<application_id>:<thread_id-or-run_id>
```

This keeps same-thread order while distributing stateless runs. Retry/dead-letter publication is acknowledged before the source offset is committed.

## Live events and history

Workers publish transient protobuf RuntimeEvents to `execution.events.v1`. Every Gateway replica has its own event consumer group, so all replicas receive the live stream and can serve their local SSE clients.

Persistence boundaries are explicit:

- live output: Kafka execution-events
- LangGraph state/history: checkpointer/store
- run lifecycle status: Runtime execution tables

Runtime does not duplicate checkpoints, token streams or state snapshots into a PostgreSQL event journal.

## FastAPI convention

```text
application/http/
  assistants/
    routes.py
    schema.py
  admin/
    audit/
      routes.py
      schema.py
```

For application routers Runtime derives folder URL prefixes, title-cased tags, nested path tags such as `Admin / Audit`, deterministic operation IDs, and validates summaries, descriptions, request schemas and examples.

LangGraph-compatible Gateway paths are protocol contracts and are never rewritten. Only tags, DTO metadata, examples and operation IDs are added.

## Scaling and failure behavior

- Gateway replicas share PostgreSQL and independently consume live-event fan-out.
- Worker replicas share one application consumer group.
- Worker capacity is bounded by a local semaphore.
- Same-thread active-run uniqueness is protected by PostgreSQL.
- Worker registry and migration ownership are durable.
- Custom FastAPI scales independently.
- Duplicate commands are ignored after a run leaves pending state.
- Failed execution retries through Kafka and then moves to dead-letter.
- A terminated migration owner leaves a recoverable claim.
- Registration retries cannot execute the same migration twice.
- Stream disconnect does not cancel a run unless cancellation is explicitly requested.
