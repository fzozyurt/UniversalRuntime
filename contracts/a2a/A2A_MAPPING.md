# A2A 1.0 Mapping Contract

UniversalRuntime uses the official A2A SDK as an edge adapter. A2A is not the worker protocol.

## Identity mapping

| A2A concept | UniversalRuntime |
|---|---|
| Agent Card | Application + active assistant metadata/capabilities |
| context/conversation ID | `thread_id` |
| task ID | `run_id` |
| task attempt | `attempt_id` internal metadata |
| message | message runtime event/input |
| artifact | artifact reference + lifecycle/runtime event |
| task status update | run lifecycle event |

## Rules

- The official SDK owns transport-specific A2A JSON-RPC, REST and gRPC shapes.
- Agent Card advertises the real endpoint and only implemented capabilities.
- A2A streaming maps ordered RuntimeEvents to official task/message/artifact updates.
- A2A cancellation calls the same run cancellation application command as native/LangGraph APIs.
- An A2A client-supplied context ID may be used directly as `thread_id` when valid; otherwise reject rather than silently create a hidden mapping.
- A2A task ID is the canonical UniversalRuntime `run_id` whenever the official SDK allows server-assigned task IDs.
- Authentication and push notifications are later security/delivery slices unless explicitly enabled and tested.
