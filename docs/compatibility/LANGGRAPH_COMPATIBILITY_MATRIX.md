# LangGraph Compatibility Matrix — Phase 1

The implementation must test with the official Python `langgraph_sdk` rather than only handwritten HTTP calls.

Legend:

- **MUST** — Phase 1 release blocker
- **SHOULD** — Phase 1 target, may be feature-flagged
- **LATER** — explicit post-MVP item

## System

| Capability | Level |
|---|---|
| health `/ok` | MUST |
| server info | MUST |
| OpenAPI document | MUST |

## Assistants

| Capability | Level |
|---|---|
| default assistant auto-registration | MUST |
| create/get/search/update/delete | MUST |
| immutable versions | MUST |
| input/output/state/config/context schemas | MUST |
| graph topology | SHOULD |
| subgraph discovery | SHOULD |

## Threads

| Capability | Level |
|---|---|
| create with supplied or generated ID | MUST |
| get/search/update/delete | MUST |
| thread status | MUST |
| current state | MUST |
| checkpoint history | MUST |
| update state | MUST |
| copy/fork | SHOULD |
| prune | LATER |

## Runs

| Capability | Level |
|---|---|
| stateful background run | MUST |
| stateful stream run | MUST |
| stateless wait/stream | MUST |
| get/list | MUST |
| cancel | MUST |
| interactive/normal/batch priority | MUST native extension |
| stream modes values/updates/messages/custom | MUST |
| events/debug/checkpoints/tasks | SHOULD based on upstream support |
| subgraph namespaces | MUST |
| interrupt/resume | MUST |
| multitask reject/enqueue | MUST |
| interrupt/rollback strategies | SHOULD |
| resumable stream/replay | SHOULD |
| webhooks | LATER |
| crons | LATER |

## Store

| Capability | Level |
|---|---|
| put/get/delete | MUST |
| search/list namespaces | MUST |
| vector search | SHOULD if pgvector available |
| TTL | LATER |

## Supported graph profiles

| Profile | Phase 1 behavior |
|---|---|
| LangGraph compiled graph | full adapter |
| LangGraph builder/factory | compile with managed persistence |
| LangChain `create_agent` | detected as LangGraph compiled graph |
| Deep Agents `create_deep_agent` | detected as LangGraph compiled graph; preserve subagent namespace/tool events |

## Mandatory compatibility scenarios

1. Official SDK creates assistant and thread.
2. Stream v1 and v2 metadata/result format.
3. Caller config, context and metadata merge correctly.
4. Supplied `thread_id` is present in LangGraph configurable config.
5. Managed Postgres state survives worker restart.
6. Interrupt emits stream event and resumes with the same run/thread semantics.
7. Nested subagent tool calls preserve namespace.
8. Stream disconnect with continue policy does not cancel execution.
9. Duplicate run command is idempotent.
10. Interactive run is dispatched ahead of queued batch work.
