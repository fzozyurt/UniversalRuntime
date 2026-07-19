# Naming and Coding Standard

## Python

- packages/modules/functions/variables: `snake_case`
- classes/protocols/exceptions: `PascalCase`
- constants: `UPPER_SNAKE_CASE`
- private implementation: `_leading_underscore`
- typed IDs: `ThreadId`, `RunId`, not raw `str` throughout the domain

## Domain vocabulary

- entity: `Run`, `Thread`, `Assistant`, `ApplicationRevision`
- value object: `ExecutionIdentity`, `QueuePriority`, `AdapterCapabilities`
- command: `CreateRunCommand`
- handler: `CreateRunHandler`
- query: `GetRunQuery`
- port: `RunRepository`, `RunCommandQueue`
- adapter: `PostgresRunRepository`, `KafkaRunCommandQueue`
- domain event: `RunCreated`
- integration event: `RunReadyV1`

Avoid vague names such as `Manager`, `Processor`, `Helper` and `Util`.

## PostgreSQL

- schema/table/column: lower snake case
- tables: plural
- primary keys: `id`
- foreign keys: `<resource>_id`
- index: `ix_<table>__<column>[_<column>]`
- unique: `uq_<table>__<column>[_<column>]`
- foreign key constraint: `fk_<table>__<target_table>`
- check: `ck_<table>__<rule>`

## Kafka

Topic:

```text
<prefix>.<environment>.<bounded-context>.<event>.v1
```

Consumer group:

```text
<prefix>.<environment>.<service>.<purpose>
```

Headers use kebab case:

- `runtime-schema-version`
- `application-id`
- `thread-id`
- `run-id`
- `attempt-id`
- `traceparent`

## Protobuf

- packages: `runtime.v1`
- messages/services: PascalCase
- fields: snake_case
- enums: upper snake case
- reserve removed fields and names
- never change semantics of an existing field in v1

## Kubernetes

```text
<prefix>-<application-key>-<component>-<revision-short>
```

Stable machine keys, not display names, form resource names.

## API

Native REST paths use plural resources and kebab-free field names. Error codes are stable uppercase snake case. Compatibility routes preserve upstream naming.
