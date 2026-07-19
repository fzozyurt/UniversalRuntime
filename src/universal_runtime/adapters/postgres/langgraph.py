from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine

from universal_runtime.adapters.postgres.locks import advisory_migration_session_lock
from universal_runtime.adapters.postgres.schema import DEFAULT_SCHEMAS, SchemaNames


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
    workspace_key: str,
    application_key: str,
    schemas: SchemaNames = DEFAULT_SCHEMAS,
) -> AsyncIterator[ManagedLangGraphPersistence]:
    try:
        from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
        from langgraph.store.postgres import AsyncPostgresStore
    except ImportError as exc:
        raise PostgresProviderUnavailableError(
            "langgraph-checkpoint-postgres and psycopg are required"
        ) from exc

    state_schema = schemas.framework_state(
        workspace_key,
        application_key,
        environment,
    )
    state_database_url = database_url_for_search_path(database_url, state_schema)
    async with migration_engine.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{state_schema}"'))
    async with AsyncPostgresSaver.from_conn_string(state_database_url) as checkpointer:
        async with AsyncPostgresStore.from_conn_string(state_database_url) as store:
            async with advisory_migration_session_lock(
                migration_engine, application_id, environment, "framework-state"
            ):
                await checkpointer.setup()
                await store.setup()
            yield ManagedLangGraphPersistence(checkpointer, store)


def database_url_for_search_path(database_url: str, schema: str) -> str:
    """Pin one LangGraph connection pool to one framework-state schema."""
    if not schema.isidentifier() or schema.lower() != schema:
        raise ValueError("framework state schema must be a lowercase SQL identifier")
    url = make_url(database_url)
    rendered = url.update_query_dict(
        {"options": f"-c search_path={schema},public"}
    ).render_as_string(hide_password=False)
    rendered = rendered.replace("postgresql+psycopg://", "postgresql://", 1)
    return rendered.replace("options=-c+search_path", "options=-c%20search_path", 1)
