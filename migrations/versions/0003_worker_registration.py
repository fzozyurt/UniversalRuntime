"""add durable worker registration fields

Revision ID: 0003_worker_registration
Revises: 0002_application_migration_coordination
Create Date: 2026-07-24
"""

from alembic import op
import sqlalchemy as sa

revision = "0003_worker_registration"
down_revision = "0002_application_migration_coordination"
branch_labels = None
depends_on = None

_SCHEMA = "rt_exec"
_TABLE = "workers"
_INDEX = "ix_rt_exec_workers_application_status"

_COLUMNS: tuple[sa.Column[object], ...] = (
    sa.Column("workspace_key", sa.String(length=255), nullable=False, server_default="default"),
    sa.Column("application_id", sa.String(length=255), nullable=False, server_default="default"),
    sa.Column("revision_id", sa.String(length=255), nullable=False, server_default="unknown"),
    sa.Column("target", sa.String(length=512), nullable=False, server_default=""),
    sa.Column("pod_name", sa.String(length=255), nullable=False, server_default=""),
    sa.Column("app_version", sa.String(length=255), nullable=False, server_default="unknown"),
    sa.Column("run_topic", sa.String(length=512), nullable=False, server_default=""),
    sa.Column("max_concurrency", sa.Integer(), nullable=False, server_default="1"),
    sa.Column("config_hash", sa.String(length=128), nullable=False, server_default=""),
    sa.Column("last_heartbeat_at", sa.DateTime(timezone=True), nullable=True),
)


def _columns() -> set[str]:
    inspector = sa.inspect(op.get_bind())
    return {column["name"] for column in inspector.get_columns(_TABLE, schema=_SCHEMA)}


def _indexes() -> set[str]:
    inspector = sa.inspect(op.get_bind())
    return {
        str(index["name"])
        for index in inspector.get_indexes(_TABLE, schema=_SCHEMA)
        if index.get("name")
    }


def upgrade() -> None:
    existing = _columns()
    for column in _COLUMNS:
        if column.name not in existing:
            op.add_column(_TABLE, column, schema=_SCHEMA)
    if _INDEX not in _indexes():
        op.create_index(_INDEX, _TABLE, ["application_id", "status"], schema=_SCHEMA)


def downgrade() -> None:
    if _INDEX in _indexes():
        op.drop_index(_INDEX, table_name=_TABLE, schema=_SCHEMA)
    existing = _columns()
    for column in reversed(_COLUMNS):
        if column.name in existing:
            op.drop_column(_TABLE, str(column.name), schema=_SCHEMA)
