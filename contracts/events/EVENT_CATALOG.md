# Runtime Event Catalog v1

## Lifecycle

- `run.queued`
- `run.started`
- `run.interrupted`
- `run.completed`
- `run.cancelled`
- `run.failed`
- `attempt.started`
- `attempt.heartbeat`
- `attempt.completed`
- `attempt.failed`

## Messages

- `message.started`
- `message.delta`
- `message.completed`

## Agents and tools

- `agent.started`
- `agent.completed`
- `agent.failed`
- `tool.started`
- `tool.progress`
- `tool.completed`
- `tool.failed`

## State

- `state.values`
- `state.updates`
- `checkpoint.created`
- `task.started`
- `task.completed`
- `interrupt.created`
- `interrupt.resolved`

Nested execution is carried in `namespace`, for example:

```json
{
  "type": "tool.started",
  "namespace": ["supervisor", "research-agent", "web-agent"]
}
```

## Custom/native preservation

Adapter-specific information is placed in `native`. Unknown native events may be emitted as `custom` while retaining their original name and payload.
