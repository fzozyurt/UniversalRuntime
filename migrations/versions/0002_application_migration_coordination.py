"""version application migration coordination

Revision ID: 0002_application_migration_coordination
Revises: 0001_platform_execution
Create Date: 2026-07-24
"""

from alembic import op
import sqlalchemy as sa

revision = "0002_application_migration_coordination"
down_revision = "0001_platform_execution"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint(
        "uq_application_migrations_application_id_workspace_key_environment",
        "application_migrations",
        schema="rt_core",
        type_="unique",
    )
    op.add_column(
        "application_migrations",
        sa.Column("target_revision", sa.String(length=255), nullable=True),
        schema="rt_core",
    )
    op.add_column(
        "application_migrations",
        sa.Column("worker_id", sa.String(length=255), nullable=True),
        schema="rt_core",
    )
    op.add_column(
        "application_migrations",
        sa.Column("attempt_number", sa.Integer(), server_default="0", nullable=False),
        schema="rt_core",
    )
    op.create_unique_constraint(
        "uq_application_migrations_target",
        "application_migrations",
        ["application_id", "workspace_key", "environment", "app_version"],
        schema="rt_core",
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_application_migrations_target",
        "application_migrations",
        schema="rt_core",
        type_="unique",
    )
    op.drop_column("application_migrations", "attempt_number", schema="rt_core")
    op.drop_column("application_migrations", "worker_id", schema="rt_core")
    op.drop_column("application_migrations", "target_revision", schema="rt_core")
    op.create_unique_constraint(
        "uq_application_migrations_application_id_workspace_key_environment",
        "application_migrations",
        ["application_id", "workspace_key", "environment"],
        schema="rt_core",
    )
