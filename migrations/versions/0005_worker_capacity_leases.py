"""enforce atomic worker capacity leases

Revision ID: 0005_worker_capacity_leases
Revises: 0004_unbound_compatibility_threads
Create Date: 2026-07-20
"""

from alembic import op
from sqlalchemy import text

revision = "0005_worker_capacity_leases"
down_revision = "0004_unbound_compatibility_threads"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    bind.execute(
        text(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_worker_leases_active_run "
            "ON rt_exec.worker_leases (run_id) WHERE acknowledged_at IS NULL"
        )
    )
    bind.execute(
        text(
            "CREATE INDEX IF NOT EXISTS ix_worker_leases_active_expiry "
            "ON rt_exec.worker_leases (expires_at) WHERE acknowledged_at IS NULL"
        )
    )
    bind.execute(
        text(
            "CREATE INDEX IF NOT EXISTS ix_worker_leases_worker_active "
            "ON rt_exec.worker_leases (worker_id) WHERE acknowledged_at IS NULL"
        )
    )


def downgrade() -> None:
    bind = op.get_bind()
    bind.execute(text("DROP INDEX IF EXISTS rt_exec.ix_worker_leases_worker_active"))
    bind.execute(text("DROP INDEX IF EXISTS rt_exec.ix_worker_leases_active_expiry"))
    bind.execute(text("DROP INDEX IF EXISTS rt_exec.uq_worker_leases_active_run"))
