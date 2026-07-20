"""create immutable application and graph revision catalog

Revision ID: 0003_control_plane_catalog
Revises: 0002_application_deployment_boundaries
Create Date: 2026-07-20
"""

from alembic import op
from sqlalchemy import text

revision = "0003_control_plane_catalog"
down_revision = "0002_application_deployment_boundaries"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    statements = (
        "ALTER TABLE rt_core.applications ADD COLUMN IF NOT EXISTS workspace_id varchar(255)",
        "ALTER TABLE rt_core.applications ADD COLUMN IF NOT EXISTS project_id varchar(255)",
        "UPDATE rt_core.applications SET workspace_id = COALESCE(workspace_id, 'default'), "
        "project_id = COALESCE(project_id, 'default')",
        "ALTER TABLE rt_core.applications ALTER COLUMN workspace_id SET NOT NULL",
        "ALTER TABLE rt_core.applications ALTER COLUMN project_id SET NOT NULL",
        "CREATE TABLE IF NOT EXISTS rt_core.application_runtime_revisions ("
        "id varchar(255) PRIMARY KEY, application_id varchar(255) NOT NULL, "
        "image_digest varchar(255) NOT NULL, descriptor_hash varchar(64) NOT NULL, "
        "metadata jsonb NOT NULL DEFAULT '{}'::jsonb, active boolean NOT NULL DEFAULT false, "
        "created_at timestamptz NOT NULL DEFAULT now(), created_by varchar(255), "
        "updated_at timestamptz NOT NULL DEFAULT now(), updated_by varchar(255), "
        "row_version integer NOT NULL DEFAULT 1)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_application_runtime_revision_digest "
        "ON rt_core.application_runtime_revisions (application_id, image_digest)",
        "CREATE INDEX IF NOT EXISTS ix_application_runtime_revision_active "
        "ON rt_core.application_runtime_revisions (application_id, active)",
        "CREATE TABLE IF NOT EXISTS rt_core.graph_revisions ("
        "id varchar(768) PRIMARY KEY, application_id varchar(255) NOT NULL, "
        "revision_id varchar(255) NOT NULL, graph_id varchar(255) NOT NULL, "
        "entrypoint varchar(1024) NOT NULL, descriptor jsonb NOT NULL, "
        "descriptor_hash varchar(64) NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), "
        "created_by varchar(255), updated_at timestamptz NOT NULL DEFAULT now(), "
        "updated_by varchar(255), row_version integer NOT NULL DEFAULT 1)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_graph_revision_identity "
        "ON rt_core.graph_revisions (application_id, revision_id, graph_id)",
        "CREATE INDEX IF NOT EXISTS ix_graph_revision_lookup "
        "ON rt_core.graph_revisions (application_id, graph_id, revision_id)",
    )
    for statement in statements:
        bind.execute(text(statement))


def downgrade() -> None:
    bind = op.get_bind()
    bind.execute(text("DROP TABLE IF EXISTS rt_core.graph_revisions"))
    bind.execute(text("DROP TABLE IF EXISTS rt_core.application_runtime_revisions"))
    # workspace/project columns are preserved because dropping tenant scope would
    # make existing application rows ambiguous.
