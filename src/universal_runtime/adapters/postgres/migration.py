from __future__ import annotations

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from universal_runtime.adapters.postgres.locks import migration_lock_key
from universal_runtime.adapters.postgres.models import PlatformBase
from universal_runtime.adapters.postgres.schema import DEFAULT_SCHEMAS, SchemaNames

logger = structlog.get_logger(__name__)


async def migrate_platform(
    engine: AsyncEngine,
    *,
    application_id: str,
    environment: str,
    schemas: SchemaNames = DEFAULT_SCHEMAS,
) -> bool:
    lock_key = migration_lock_key(application_id, environment, "platform")
    async with engine.begin() as connection:
        result = await connection.execute(
            text("SELECT pg_try_advisory_xact_lock(:lock_key)"),
            {"lock_key": lock_key},
        )
        acquired = result.scalar()
        if not acquired:
            logger.info(
                "migration.skipped",
                reason="lock_busy",
                application_id=application_id,
                environment=environment,
            )
            return False
        await connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schemas.core}"'))
        await connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schemas.execution}"'))
        await connection.run_sync(PlatformBase.metadata.create_all)
    logger.info(
        "migration.completed",
        schema_core=schemas.core,
        schema_execution=schemas.execution,
        application_id=application_id,
        environment=environment,
    )
    return True
