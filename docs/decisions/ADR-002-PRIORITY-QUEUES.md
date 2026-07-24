# ADR-002: Application-owned Kafka priority topics

## Status

Accepted for Phase 1; supersedes the Dispatcher-based design.

## Decision

Use separate short and long run topics for every application deployment:

```text
rt.<environment>.<application_id>.runs.short_queue.v1
rt.<environment>.<application_id>.runs.long_queue.v1
```

Gateway publishes directly to the topic selected from run priority. Worker replicas for the same application subscribe to both topics through one shared consumer group. There is no Dispatcher service.

## Rationale

Kafka consumer groups already provide scalable work sharing and partition ownership. An additional Dispatcher pod introduced another availability boundary, obscured scaling behavior and could not guarantee equal load distribution.

Application-scoped topics provide:

- deterministic Gateway routing,
- independent application scaling,
- direct consumer-group rebalancing,
- no central worker-selection bottleneck,
- a smaller deployment surface.

## Ordering

The partition key is:

```text
<application_id>:<thread_id-or-run_id>
```

Same-thread commands remain ordered in one partition. Stateless commands distribute by run ID. PostgreSQL still prevents multiple active runs for one thread.

## Retry and dead-letter

A failed command is republished to its source topic until the retry limit is reached. It is then copied to the application dead-letter topic. The worker commits the source offset only after Kafka acknowledges the retry or dead-letter publication.

## Consequences

- Each application owns its topic set and consumer group.
- Worker replicas must use identical application/environment/topic configuration.
- Priority fairness is handled through separate topics and worker consumption policy rather than a central scheduler.
- Topic provisioning must use the canonical naming helper.
