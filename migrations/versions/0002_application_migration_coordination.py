"""version application migration coordination

Revision ID: 0002_application_migration_coordination
Revises: 0001_platform_execution
Create Date: 2026-07-24
"""

import sqlalchemy as sa
from alembic import op

revision = "0002_application_migration_coordination"
down_revision = "0001_platform_execution"
branch_labels = None
depends_on = None

_SCHEMA = "rt_core"
_TABLE = "application_migrations"
_OLD_UNIQUE = "uq_application_migrations_application_id_workspace_key_environment"
_NEW_UNIQUE = "uq_application_migrations_target"


def _columns() -> set[str]:
    inspector = sa.inspect(op.get_bind())
    return {column["name"] for column in inspector.get_columns(_TABLE, schema=_SCHEMA)}


def _unique_constraints() -> set[str]:
    inspector = sa.inspect(op.get_bind())
    return {
        str(constraint["name"])
        for constraint in inspector.get_unique_constraints(_TABLE, schema=_SCHEMA)
        if constraint.get("name")
    }


def upgrade() -> None:
    columns = _columns()
    constraints = _unique_constraints()

    if _OLD_UNIQUE in constraints:
        op.drop_constraint(_OLD_UNIQUE, _TABLE, schema=_SCHEMA, type_="unique")
    if "target_revision" not in columns:
        op.add_column(
            _TABLE,
            sa.Column("target_revision", sa.String(length=255), nullable=True),
            schema=_SCHEMA,
        )
    if "worker_id" not in columns:
        op.add_column(
            _TABLE,
            sa.Column("worker_id", sa.String(length=255), nullable=True),
            schema=_SCHEMA,
        )
    if "attempt_number" not in columns:
        op.add_column(
            _TABLE,
            sa.Column("attempt_number", sa.Integer(), server_default="0", nullable=False),
            schema=_SCHEMA,
        )
    if _NEW_UNIQUE not in constraints:
        op.create_unique_constraint(
            _NEW_UNIQUE,
            _TABLE,
            ["application_id", "workspace_key", "environment", "app_version"],
            schema=_SCHEMA,
        )


def downgrade() -> None:
    columns = _columns()
    constraints = _unique_constraints()

    if _NEW_UNIQUE in constraints:
        op.drop_constraint(_NEW_UNIQUE, _TABLE, schema=_SCHEMA, type_="unique")
    if "attempt_number" in columns:
        op.drop_column(_TABLE, "attempt_number", schema=_SCHEMA)
    if "worker_id" in columns:
        op.drop_column(_TABLE, "worker_id", schema=_SCHEMA)
    if "target_revision" in columns:
        op.drop_column(_TABLE, "target_revision", schema=_SCHEMA)
    if _OLD_UNIQUE not in constraints:
        op.create_unique_constraint(
            _OLD_UNIQUE,
            _TABLE,
            ["application_id", "workspace_key", "environment"],
            schema=_SCHEMA,
        )
