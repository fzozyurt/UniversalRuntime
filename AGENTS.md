# AGENTS.md — UniversalRuntime Engineering Contract

## 1. Mission

UniversalRuntime is a pluggable, Kubernetes-native runtime for streaming AI applications. Phase 1 must provide a production-shaped LangGraph-compatible MVP while remaining framework-neutral at the core.

The mandatory Phase 1 runtime family is:

- native LangGraph compiled graphs,
- LangChain `create_agent` outputs,
- Deep Agents `create_deep_agent` outputs.

All three resolve to the same LangGraph runtime adapter.

## 2. Non-negotiable product behavior

The minimum universal runtime behavior is:

1. create or identify an application/assistant,
2. create a thread,
3. start a run,
4. stream incremental events,
5. cancel a run,
6. expose health and capability information.

Checkpoint, state, history, HITL, fork, A2A and custom HTTP are adapter capabilities. They must never pollute the framework-neutral core.

## 3. Architecture rules

### 3.1 Hexagonal boundaries

The dependency direction is strict:

```text
Domain <- Application <- Ports <- Adapters/Infrastructure <- Bootstrap
```

- `domain/` imports only the Python standard library and typing primitives.
- `application/` may import domain and ports.
- `ports/` contains protocols/interfaces only.
- `adapters/` may import frameworks and infrastructure clients.
- `bootstrap/` is the composition root and is the only place that wires concrete implementations.
- Gateway API DTOs, SQLAlchemy rows, protobuf messages and LangGraph objects must not become domain entities.

### 3.2 No generic dumping grounds

Do not create or grow these directories/modules:

- `utils`
- `helpers`
- `common`
- `misc`
- `manager`
- `base` without a precise bounded meaning

Every reusable element must have a clear domain or technical ownership.

### 3.3 Adapter capability model

Every adapter must publish an immutable manifest:

- adapter identifier and version,
- supported profiles,
- streaming modes,
- custom thread/run ID support,
- checkpoint/state/history/HITL capabilities,
- session affinity requirement,
- custom HTTP and A2A support.

Unsupported operations return a typed `CAPABILITY_NOT_SUPPORTED` error. Do not emulate state/checkpoint semantics that the framework does not provide.

### 3.4 Compatibility and native APIs are separate

- `/compat/langgraph/*` must preserve LangGraph request, response, HTTP status and SSE behavior.
- `/api/v1/*` may use UniversalRuntime response envelopes.
- Never wrap compatibility responses with the native `{data, meta}` envelope.

## 4. Canonical identities

Use UUIDv7-compatible string IDs where the language/runtime supports them:

- `workspace_id`
- `project_id`
- `application_id`
- `revision_id`
- `deployment_id`
- `assistant_id`
- `thread_id`
- `run_id`
- `attempt_id`
- `event_id`

Rules:

- `thread_id` is the conversation/execution lineage.
- `run_id` is one invocation.
- `attempt_id` is one worker execution attempt.
- `run_id` always belongs to UniversalRuntime.
- Pass canonical IDs directly to frameworks whenever supported.
- Create an identity binding only if a framework forces an opaque native session ID.

## 5. Phase 1 execution model

### Local profile

- single process or standalone launcher,
- InMemory repositories,
- `asyncio.PriorityQueue`,
- LangGraph `InMemorySaver` and `InMemoryStore`,
- direct async execution,
- optional local FastAPI application mount.

### Production profile

- Gateway receives public requests,
- PostgreSQL is authoritative metadata/state,
- transactional outbox emits run commands,
- Kafka transports commands/events,
- Dispatcher leases runs and selects workers,
- Worker communicates over gRPC,
- LangGraph uses managed `AsyncPostgresSaver` and `AsyncPostgresStore`,
- Event projector persists lifecycle/structured outputs and can fan out to OpenSearch or analytics sinks.

## 6. Queue and latency rules

Priorities:

```text
interactive = 100
normal      = 50
batch       = 10
```

- Chat and synchronous UI requests default to `interactive`.
- Scheduled bulk work defaults to `batch`.
- Production Kafka uses separate priority topics because Kafka does not reorder records inside one partition.
- Dispatcher uses weighted fair scheduling, for example `8 interactive : 3 normal : 1 batch`, plus age promotion to avoid starvation.
- Kafka partition key is `application_id:thread_id`.
- Only one active run per thread unless a future explicit multitask strategy allows otherwise.

## 7. Async and concurrency rules

All I/O ports are async.

Worker concurrency is bounded by:

1. application deployment config,
2. `UR_WORKER_MAX_CONCURRENCY` ENV override,
3. platform policy ceiling.

Use one process-level `asyncio.Semaphore`. Never start unbounded tasks. Graceful shutdown must:

- stop accepting leases,
- wait for active executions up to drain timeout,
- checkpoint/cancel according to policy,
- release or expire leases,
- close Kafka, PostgreSQL, gRPC and HTTP clients.

## 8. Config contract

Application config is defined by `runtime.yaml` and validated against `contracts/config/runtime-application.schema.json`.

Config precedence from lowest to highest:

```text
SDK defaults
runtime.yaml
secret/service bindings
Gateway deployment override
platform policy
```

Gateway-managed edits create immutable config revisions with:

- revision number,
- canonical JSON,
- SHA-256 config hash,
- creator,
- created timestamp,
- activation timestamp.

Environment interpolation supports only:

- `${VAR}`
- `${VAR:-default}`
- `${VAR:?required message}`

Do not support arbitrary code or template execution.

## 9. LangGraph adapter rules

### 9.1 Supported object forms

Detection priority:

1. explicit entrypoint in `runtime.yaml`,
2. exported compiled graph,
3. exported graph builder/factory,
4. LangChain `create_agent` output,
5. Deep Agents `create_deep_agent` output,
6. isolated import scan when enabled.

Control Plane/Gateway must never import user code. Inspection occurs inside the application image or isolated build container.

### 9.2 Managed persistence

For `platform-managed` mode:

- user graph should not provide a production checkpointer,
- SDK injects `AsyncPostgresSaver` and `AsyncPostgresStore` in production,
- SDK injects `InMemorySaver` and `InMemoryStore` locally,
- call upstream `setup()` under a PostgreSQL advisory migration lock,
- do not fork or alter upstream checkpoint tables.

Current Python Postgres saver schema targeting may rely on a dedicated DB role with a fixed `search_path` until explicit schema selection is available and verified. One pool must not switch schemas dynamically across applications.

### 9.3 Runtime config injection

Merge caller config without overwriting protected runtime values. Inject:

```python
{
    "run_id": run_id,
    "configurable": {
        "thread_id": thread_id,
        "checkpoint_ns": checkpoint_namespace,
        "checkpoint_id": checkpoint_id_if_present,
        "assistant_id": assistant_id,
        "run_id": run_id,
    },
    "metadata": {
        "runtime.application_id": application_id,
        "runtime.revision_id": revision_id,
        "runtime.deployment_id": deployment_id,
        "runtime.run_id": run_id,
        "runtime.attempt_id": attempt_id,
    },
}
```

Pass request `context` separately to modern LangGraph APIs. Preserve caller `tags`, `recursion_limit`, configurable values and metadata unless they collide with protected keys.

### 9.4 Streaming

Preserve LangGraph-compatible stream modes including:

- `values`
- `updates`
- `messages`
- `messages-tuple`
- `custom`
- `events`
- `debug`
- `checkpoints`
- `tasks`

Internal canonical events add IDs, sequence and namespace. Compatibility responses convert them back to the exact expected SSE form.

Subagent/tool nesting is represented by `namespace`, not by inventing combinatorial event names.

## 10. Database rules

Logical schemas:

- `rt_core`: applications, revisions, deployments, assistants and config revisions.
- `rt_exec`: threads, runs, attempts, commands, event batches, outbox/inbox and artifacts.
- `rt_s_<workspace_key>_<application_key>_<environment>`: framework checkpoint/store state.
- `rt_a_<workspace_key>_<application_key>_<environment>`: application/FastAPI tables.

The prefix `rt` is configurable, but the suffix naming contract is stable.

Every mutable platform table uses shared mixins for:

- `created_at`
- `created_by`
- `updated_at`
- `updated_by`
- optional optimistic version

Do not duplicate audit columns manually in every model.

Migration categories:

1. platform Alembic migrations,
2. framework native state migrations,
3. application Alembic migrations.

Every migration category obtains an advisory lock using a deterministic application/environment/migration key.

## 11. Kafka contract

Default topic names use:

```text
<prefix>.<environment>.<bounded-context>.<event>.v1
```

Defaults:

- `rt.<env>.runs.interactive.v1`
- `rt.<env>.runs.normal.v1`
- `rt.<env>.runs.batch.v1`
- `rt.<env>.execution.events.v1`
- `rt.<env>.run.lifecycle.v1`
- `rt.<env>.run.commands.v1`
- `rt.<env>.deadletter.v1`

All names are overridable independently. Changing only the prefix must preserve the remaining suffixes.

Do not commit a consumed run command until the authoritative run transition and required output/event persistence have succeeded.

## 12. gRPC and protobuf rules

- Protobuf is the internal serialization contract.
- Public clients use HTTP/REST/SSE.
- Use `google.protobuf.Value` for arbitrary JSON values that may be scalar, list, object or null.
- Use `google.protobuf.Struct` for object-only config/context/metadata.
- Use standard `grpc.health.v1.Health` for health checks.
- Breaking field reuse is forbidden; reserve removed field numbers and names.
- Additive changes only within v1.

## 13. FastAPI custom HTTP

- Explicit entrypoint wins over detection.
- Detection uses static AST first, isolated import second.
- Gateway proxies custom paths under a stable application prefix.
- Correctly propagate `root_path`, forwarded host/proto/prefix and trace context.
- Gateway and application routes share internal request identity but remain operationally separate.
- Custom application database migrations use the app schema, never checkpoint schemas.

## 14. A2A

Phase 1 A2A is an adapter surface, not the internal runtime protocol.

- publish an Agent Card,
- advertise only implemented capabilities,
- map A2A context/conversation to `thread_id`,
- map A2A task to `run_id`,
- map artifacts and status updates to canonical runtime events,
- support streaming only when the adapter capability says so.

## 15. Observability

- OpenTelemetry is the internal standard.
- Zero-code launcher is enabled only when `UR_OBSERVABILITY_ENABLED=true`.
- OTLP endpoint/protocol/headers are environment-configurable.
- Optional OpenLIT initialization must remain in a telemetry adapter, not domain/application code.
- FastAPI, HTTPX, gRPC, SQLAlchemy, Kafka and LangChain/LangGraph spans must share trace context.
- Background worker executions create a root/linked `runtime.run` span.
- Exceptions set span status, record exception and produce a structured error event.
- Never place secrets or unrestricted prompts/tool results into span attributes.

## 16. API errors

All native API errors use:

```json
{
  "error": {
    "code": "CAPABILITY_NOT_SUPPORTED",
    "message": "...",
    "retryable": false,
    "request_id": "...",
    "details": {}
  }
}
```

Suggested status mapping:

- validation: 400/422
- not found: 404
- state/thread conflict: 409
- capability/adapter unsupported: 501
- rate limit/backpressure: 429
- dependency temporarily unavailable: 503
- execution timeout: 504
- unexpected: 500

Compatibility endpoints follow LangGraph’s expected errors instead of this envelope.

## 17. Security baseline

Even before full Auth/RBAC:

- no application pod is public,
- no Kubernetes token is mounted by default,
- no Docker socket or hostPath,
- no raw user JWT forwarded to worker,
- secrets resolve only inside workload scope,
- secret values are redacted from config dumps/logs/traces,
- internal Gateway↔Worker communication is prepared for mTLS or signed short-lived tokens,
- application DB role has access only to its schemas.

## 18. Quality gates

Mandatory before a PR is reviewable:

```bash
ruff check .
ruff format --check .
mypy src services
pytest -q --cov=src --cov=services --cov-report=term-missing --cov-fail-under=80
python scripts/check_architecture.py
python scripts/validate_contracts.py
```

Additionally:

- resmî `langgraph_sdk` compatibility suite,
- local InMemory E2E,
- PostgreSQL/Kafka integration tests when services are available,
- package build and container smoke tests.

## 19. Definition of done

A task is not done because files exist. It is done only when:

- contract and implementation agree,
- tests cover success, failure and cancellation,
- errors are typed,
- observability is present,
- docs and examples are updated,
- no hidden global state or unbounded async task remains,
- compatibility fixtures are updated intentionally,
- all quality gates pass.
