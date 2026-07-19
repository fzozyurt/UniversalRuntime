from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from sqlalchemy.ext.asyncio import AsyncEngine

from universal_runtime.adapters.postgres.locks import advisory_migration_lock


class PostgresProviderUnavailableError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ManagedLangGraphPersistence:
    checkpointer: Any
    store: Any


@asynccontextmanager
async def managed_langgraph_persistence(
    database_url: str,
    *,
    migration_engine: AsyncEngine,
    application_id: str,
    environment: str,
) -> AsyncIterator[ManagedLangGraphPersistence]:
    try:
        from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
        from langgraph.store.postgres import AsyncPostgresStore
    except ImportError as exc:
        raise PostgresProviderUnavailableError(
            "langgraph-checkpoint-postgres and psycopg are required"
        ) from exc

    async with AsyncPostgresSaver.from_conn_string(database_url) as checkpointer:
        async with AsyncPostgresStore.from_conn_string(database_url) as store:
            async with advisory_migration_lock(
                migration_engine, application_id, environment, "framework-state"
            ):
                await checkpointer.setup()
                await store.setup()
            yield ManagedLangGraphPersistence(checkpointer, store)
