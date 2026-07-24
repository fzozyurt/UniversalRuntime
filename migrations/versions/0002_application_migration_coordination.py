"""version application migration coordination

Revision ID: 0002_worker_coordination
Revises: 0001_platform_execution
Create Date: 2026-07-24
"""

import sqlalchemy as sa
from alembic import op

revision = "0002_worker_coordination"
down_revision = "0001_platform_execution"
branch_labels = None
depends_on = None

_SCHEMA = "rt_core"
_TABLE = "application_migrations"
_OLD_COLUMNS = ("application_id", "workspace_key", "environment")
_NEW_COLUMNS = (*_OLD_COLUMNS, "app_version")
_OLD_UNIQUE = "uq_application_migrations_application_id_workspace_key_environment"
_NEW_UNIQUE = "uq_application_migrations_target"


def _columns() -> set[str]:
    inspector = sa.inspect(op.get_bind())
    return {column["name"] for column in inspector.get_columns(_TABLE, schema=_SCHEMA)}


def _unique_constraints() -> list[dict[str, object]]:
    inspector = sa.inspect(op.get_bind())
    return list(inspector.get_unique_constraints(_TABLE, schema=_SCHEMA))


def _constraint_names_for(columns: tuple[str, ...]) -> list[str]:
    expected = set(columns)
    return [
        str(constraint["name"])
        for constraint in _unique_constraints()
        if constraint.get("name") and set(constraint.get("column_names") or ()) == expected
    ]


def upgrade() -> None:
    columns = _columns()
    for constraint_name in _constraint_names_for(_OLD_COLUMNS):
        op.drop_constraint(constraint_name, _TABLE, schema=_SCHEMA, type_="unique")

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
    if not _constraint_names_for(_NEW_COLUMNS):
        op.create_unique_constraint(
            _NEW_UNIQUE,
            _TABLE,
            list(_NEW_COLUMNS),
            schema=_SCHEMA,
        )


def downgrade() -> None:
    columns = _columns()
    for constraint_name in _constraint_names_for(_NEW_COLUMNS):
        op.drop_constraint(constraint_name, _TABLE, schema=_SCHEMA, type_="unique")
    if "attempt_number" in columns:
        op.drop_column(_TABLE, "attempt_number", schema=_SCHEMA)
    if "worker_id" in columns:
        op.drop_column(_TABLE, "worker_id", schema=_SCHEMA)
    if "target_revision" in columns:
        op.drop_column(_TABLE, "target_revision", schema=_SCHEMA)
    if not _constraint_names_for(_OLD_COLUMNS):
        op.create_unique_constraint(
            _OLD_UNIQUE,
            _TABLE,
            list(_OLD_COLUMNS),
            schema=_SCHEMA,
        )
