from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from universal_runtime.adapters.postgres.locks import advisory_migration_lock
from universal_runtime.adapters.postgres.models import PlatformBase
from universal_runtime.adapters.postgres.schema import DEFAULT_SCHEMAS, SchemaNames


async def migrate_platform(
    engine: AsyncEngine,
    *,
    application_id: str,
    environment: str,
    schemas: SchemaNames = DEFAULT_SCHEMAS,
) -> None:
    async with advisory_migration_lock(
        engine, application_id, environment, "platform"
    ) as connection:
        await connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schemas.core}"'))
        await connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schemas.execution}"'))
        await connection.run_sync(PlatformBase.metadata.create_all)
