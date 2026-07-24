from __future__ import annotations

from pathlib import Path
from typing import Any

import structlog
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from universal_runtime.adapters.postgres.locks import advisory_migration_lock
from universal_runtime.adapters.postgres.schema import DEFAULT_SCHEMAS, SchemaNames

logger = structlog.get_logger(__name__)
_PLATFORM_MIGRATIONS = Path(__file__).resolve().parents[4] / "migrations"


async def migrate_platform(
    engine: AsyncEngine,
    *,
    application_id: str,
    environment: str,
    schemas: SchemaNames = DEFAULT_SCHEMAS,
) -> bool:
    """Upgrade the shared Runtime schema under one blocking advisory lock.

    Every Gateway replica may call this during startup. The lock serializes the
    upgrade and all replicas return only after the authoritative Alembic head is
    installed; no replica becomes ready while another migration is in flight.
    """

    config = Config()
    config.set_main_option("script_location", str(_PLATFORM_MIGRATIONS))
    config.set_main_option("version_table_schema", schemas.core)
    config.attributes["application_id"] = application_id
    config.attributes["environment"] = environment
    config.attributes["migration_lock_acquired"] = True

    async with advisory_migration_lock(
        engine,
        application_id,
        environment,
        "platform",
    ) as connection:
        await connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schemas.core}"'))
        await connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schemas.execution}"'))

        def _upgrade(sync_connection: Any) -> None:
            config.attributes["connection"] = sync_connection
            command.upgrade(config, "head")

        await connection.run_sync(_upgrade)

    logger.info(
        "migration.completed",
        schema_core=schemas.core,
        schema_execution=schemas.execution,
        application_id=application_id,
        environment=environment,
    )
    return True
