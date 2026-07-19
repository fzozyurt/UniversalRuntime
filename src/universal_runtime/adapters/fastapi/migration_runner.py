from __future__ import annotations

import asyncio
from typing import Any

from alembic import command
from alembic.config import Config

from universal_runtime.adapters.postgres.locks import advisory_migration_lock
from universal_runtime.domain.errors import ErrorCode, RuntimeFailure


class AlembicApplicationMigrationRunner:
    def __init__(self, engine: Any) -> None:
        self.engine = engine

    async def upgrade(self, *, config_path: str, application_id: str, environment: str) -> None:
        try:
            async with advisory_migration_lock(
                self.engine, application_id, environment, "application"
            ):
                await asyncio.to_thread(command.upgrade, Config(config_path), "head")
        except Exception as exc:
            raise RuntimeFailure(
                ErrorCode.APPLICATION_MIGRATION_FAILED, "application migration failed"
            ) from exc
