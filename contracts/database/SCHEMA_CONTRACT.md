# Database Schema Contract v1

## Shared schemas

### `<prefix>_core`

- applications
- application_revisions
- deployments
- deployment_config_revisions
- graphs
- assistants
- assistant_versions

### `<prefix>_exec`

- threads
- runs
- run_attempts
- run_commands
- interrupts
- runtime_event_batches
- run_lifecycle_events
- workers
- worker_leases
- outbox_events
- inbox_events
- artifacts

## Application-specific schemas

```text
<prefix>_s_<workspace_key>_<application_key>_<environment>
<prefix>_a_<workspace_key>_<application_key>_<environment>
```

- `_s_`: native framework state/checkpoints/store.
- `_a_`: custom FastAPI/application SQLAlchemy tables.

## Invariants

- one active leased/executing run per thread,
- unique `(run_id, attempt_number)`,
- immutable application revision and assistant version,
- immutable outbox event identity,
- event sequence unique per run,
- configuration hash unique per application revision where appropriate.

## Migration lock

Use PostgreSQL advisory lock keyed by:

```text
sha256(application_id + environment + migration_category)
```

Migration categories:

- `platform`
- `framework-state`
- `application`

## Audit mixin

All mutable platform records inherit common columns:

- `created_at timestamptz not null`
- `created_by text null`
- `updated_at timestamptz not null`
- `updated_by text null`
- `row_version integer not null default 1`
