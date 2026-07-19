"""separate application deployment and executable graph boundaries

Revision ID: 0002_application_deployment_boundaries
Revises: 0001_platform_execution
Create Date: 2026-07-20
"""

from alembic import op
from sqlalchemy import text

revision = "0002_application_deployment_boundaries"
down_revision = "0001_platform_execution"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    statements = (
        "ALTER TABLE rt_core.deployments ADD COLUMN IF NOT EXISTS revision_id varchar(255)",
        "UPDATE rt_core.deployments SET revision_id = COALESCE(revision_id, 'active')",
        "ALTER TABLE rt_core.deployments ALTER COLUMN revision_id SET NOT NULL",
        "ALTER TABLE rt_core.graphs ADD COLUMN IF NOT EXISTS revision_id varchar(255)",
        "ALTER TABLE rt_core.graphs ADD COLUMN IF NOT EXISTS entrypoint varchar(1024)",
        "UPDATE rt_core.graphs SET revision_id = COALESCE(revision_id, 'active'), "
        "entrypoint = COALESCE(entrypoint, graph_id)",
        "ALTER TABLE rt_core.graphs ALTER COLUMN revision_id SET NOT NULL",
        "ALTER TABLE rt_core.graphs ALTER COLUMN entrypoint SET NOT NULL",
        "ALTER TABLE rt_exec.threads ADD COLUMN IF NOT EXISTS workspace_id varchar(255)",
        "ALTER TABLE rt_exec.threads ADD COLUMN IF NOT EXISTS project_id varchar(255)",
        "ALTER TABLE rt_exec.threads ADD COLUMN IF NOT EXISTS application_id varchar(255)",
        "UPDATE rt_exec.threads SET workspace_id = COALESCE(workspace_id, 'default'), "
        "project_id = COALESCE(project_id, 'default'), application_id = COALESCE(application_id, 'default')",
        "ALTER TABLE rt_exec.threads ALTER COLUMN workspace_id SET NOT NULL",
        "ALTER TABLE rt_exec.threads ALTER COLUMN project_id SET NOT NULL",
        "ALTER TABLE rt_exec.threads ALTER COLUMN application_id SET NOT NULL",
        "ALTER TABLE rt_exec.runs ADD COLUMN IF NOT EXISTS graph_id varchar(255)",
        "ALTER TABLE rt_exec.runs ADD COLUMN IF NOT EXISTS assistant_version integer",
        "UPDATE rt_exec.runs SET graph_id = COALESCE(graph_id, assistant_id, 'default'), "
        "assistant_version = COALESCE(assistant_version, 1)",
        "ALTER TABLE rt_exec.runs ALTER COLUMN graph_id SET NOT NULL",
        "ALTER TABLE rt_exec.runs ALTER COLUMN assistant_version SET NOT NULL",
        "ALTER TABLE rt_exec.workers ADD COLUMN IF NOT EXISTS application_id varchar(255)",
        "ALTER TABLE rt_exec.workers ADD COLUMN IF NOT EXISTS revision_id varchar(255)",
        "ALTER TABLE rt_exec.workers ADD COLUMN IF NOT EXISTS grpc_target varchar(1024)",
        "ALTER TABLE rt_exec.workers ADD COLUMN IF NOT EXISTS graph_ids jsonb",
        "ALTER TABLE rt_exec.workers ADD COLUMN IF NOT EXISTS max_concurrency integer",
        "ALTER TABLE rt_exec.workers ADD COLUMN IF NOT EXISTS active_executions integer",
        "ALTER TABLE rt_exec.workers ADD COLUMN IF NOT EXISTS available_slots integer",
        "ALTER TABLE rt_exec.workers ADD COLUMN IF NOT EXISTS last_heartbeat_at timestamptz",
        "ALTER TABLE rt_exec.workers ADD COLUMN IF NOT EXISTS expires_at timestamptz",
        "UPDATE rt_exec.workers SET application_id = COALESCE(application_id, 'default'), "
        "revision_id = COALESCE(revision_id, 'active'), grpc_target = COALESCE(grpc_target, ''), "
        "graph_ids = COALESCE(graph_ids, '[]'::jsonb), max_concurrency = COALESCE(max_concurrency, 1), "
        "active_executions = COALESCE(active_executions, 0), available_slots = COALESCE(available_slots, 1), "
        "last_heartbeat_at = COALESCE(last_heartbeat_at, now()), "
        "expires_at = COALESCE(expires_at, now() + interval '60 seconds')",
        "ALTER TABLE rt_exec.workers ALTER COLUMN application_id SET NOT NULL",
        "ALTER TABLE rt_exec.workers ALTER COLUMN revision_id SET NOT NULL",
        "ALTER TABLE rt_exec.workers ALTER COLUMN grpc_target SET NOT NULL",
        "ALTER TABLE rt_exec.workers ALTER COLUMN graph_ids SET NOT NULL",
        "ALTER TABLE rt_exec.workers ALTER COLUMN max_concurrency SET NOT NULL",
        "ALTER TABLE rt_exec.workers ALTER COLUMN active_executions SET NOT NULL",
        "ALTER TABLE rt_exec.workers ALTER COLUMN available_slots SET NOT NULL",
        "ALTER TABLE rt_exec.workers ALTER COLUMN last_heartbeat_at SET NOT NULL",
        "ALTER TABLE rt_exec.workers ALTER COLUMN expires_at SET NOT NULL",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_graphs_application_graph "
        "ON rt_core.graphs (application_id, graph_id)",
        "CREATE INDEX IF NOT EXISTS ix_threads_application_created "
        "ON rt_exec.threads (application_id, created_at)",
        "CREATE INDEX IF NOT EXISTS ix_threads_application_status "
        "ON rt_exec.threads (application_id, status)",
        "CREATE INDEX IF NOT EXISTS ix_runs_deployment_status "
        "ON rt_exec.runs (deployment_id, status)",
        "CREATE INDEX IF NOT EXISTS ix_workers_deployment_status "
        "ON rt_exec.workers (deployment_id, status)",
    )
    for statement in statements:
        bind.execute(text(statement))


def downgrade() -> None:
    # This boundary migration intentionally preserves identity and routing data.
    # Destructive downgrade would make existing runs and worker registrations ambiguous.
    pass
