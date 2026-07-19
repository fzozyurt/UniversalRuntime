# Event Projector

Consumes `run.lifecycle` and selected structured runtime events.

Built-in projections:

- final run output/status into PostgreSQL,
- event batches for replay,
- optional OpenSearch analytics sink,
- optional custom sink plugin.

The execution path must not synchronously wait for analytics sinks after durable event publication.
