# Kafka Topic Contract v1

## Defaults

| Purpose | Default topic |
|---|---|
| interactive runs | `rt.<env>.runs.interactive.v1` |
| normal runs | `rt.<env>.runs.normal.v1` |
| batch runs | `rt.<env>.runs.batch.v1` |
| commands | `rt.<env>.run.commands.v1` |
| runtime stream events | `rt.<env>.execution.events.v1` |
| lifecycle/projection events | `rt.<env>.run.lifecycle.v1` |
| audit | `rt.<env>.audit.events.v1` |
| dead letter | `rt.<env>.deadletter.v1` |

Every topic is individually overridable. Prefix-only override recalculates defaults.

## Key

```text
<application_id>:<thread_id>
```

Stateless runs use `<application_id>:<run_id>`.

## Required headers

- `runtime-schema-version: 1`
- `event-id`
- `application-id`
- `revision-id`
- `deployment-id`
- `thread-id` when stateful
- `run-id`
- `attempt-id` when assigned
- `traceparent` when present
- `content-type: application/x-protobuf` or `application/json`

## Delivery

- Producers use idempotent delivery where supported.
- Consumers maintain an inbox/dedup record by `event_id`.
- Run command offset is committed after authoritative transition and required persistence.
- Poison records go to dead letter with original metadata and normalized error code.
