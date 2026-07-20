"""allow compatibility threads to bind to an application on first run

Revision ID: 0004_unbound_compatibility_threads
Revises: 0003_control_plane_catalog
Create Date: 2026-07-20
"""

from alembic import op
from sqlalchemy import text

revision = "0004_unbound_compatibility_threads"
down_revision = "0003_control_plane_catalog"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.get_bind().execute(
        text("ALTER TABLE rt_exec.threads ALTER COLUMN application_id DROP NOT NULL")
    )


def downgrade() -> None:
    bind = op.get_bind()
    unbound = bind.execute(
        text("SELECT COUNT(*) FROM rt_exec.threads WHERE application_id IS NULL")
    ).scalar_one()
    if unbound:
        raise RuntimeError(
            "cannot restore NOT NULL application_id while unbound compatibility threads exist"
        )
    bind.execute(
        text("ALTER TABLE rt_exec.threads ALTER COLUMN application_id SET NOT NULL")
    )
