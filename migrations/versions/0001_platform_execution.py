"""create platform and execution tables

Revision ID: 0001_platform_execution
Revises:
Create Date: 2026-07-19
"""

from alembic import op
from sqlalchemy import text

from universal_runtime.adapters.postgres.models import PlatformBase
from universal_runtime.adapters.postgres.schema import DEFAULT_SCHEMAS

revision = "0001_platform_execution"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    bind.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{DEFAULT_SCHEMAS.core}"'))
    bind.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{DEFAULT_SCHEMAS.execution}"'))
    PlatformBase.metadata.create_all(bind=bind, checkfirst=True)


def downgrade() -> None:
    bind = op.get_bind()
    PlatformBase.metadata.drop_all(bind=bind, checkfirst=True)
