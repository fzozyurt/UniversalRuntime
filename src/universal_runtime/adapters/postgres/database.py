from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from universal_runtime.adapters.postgres.models import PlatformBase
from universal_runtime.adapters.postgres.schema import DEFAULT_SCHEMAS, SchemaNames


def create_engine(
    database_url: str,
    *,
    search_path: str | None = None,
    pool_size: int = 5,
    max_overflow: int = 10,
) -> AsyncEngine:
    engine = create_async_engine(
        database_url,
        pool_pre_ping=True,
        pool_size=pool_size,
        max_overflow=max_overflow,
    )
    if search_path is not None:
        _install_fixed_search_path(engine, search_path)
    return engine


def create_application_engine(
    database_url: str,
    *,
    workspace_key: str,
    application_key: str,
    environment: str,
    schemas: SchemaNames = DEFAULT_SCHEMAS,
    pool_size: int = 5,
    max_overflow: int = 10,
) -> AsyncEngine:
    """Create a pool pinned to one application's schema."""
    schema = schemas.application(workspace_key, application_key, environment)
    return create_engine(
        database_url,
        search_path=f"{schema},public",
        pool_size=pool_size,
        max_overflow=max_overflow,
    )


def create_framework_state_engine(
    database_url: str,
    *,
    workspace_key: str,
    application_key: str,
    environment: str,
    schemas: SchemaNames = DEFAULT_SCHEMAS,
    pool_size: int = 5,
    max_overflow: int = 10,
) -> AsyncEngine:
    """Create a pool pinned to one application's framework state schema."""
    schema = schemas.framework_state(workspace_key, application_key, environment)
    return create_engine(
        database_url,
        search_path=f"{schema},public",
        pool_size=pool_size,
        max_overflow=max_overflow,
    )


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


async def create_schemas(engine: AsyncEngine, schemas: SchemaNames = DEFAULT_SCHEMAS) -> None:
    async with engine.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schemas.core}"'))
        await connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schemas.execution}"'))


async def create_platform_tables(engine: AsyncEngine) -> None:
    await create_schemas(engine)
    async with engine.begin() as connection:
        await connection.run_sync(PlatformBase.metadata.create_all)


async def dispose_engine(engine: AsyncEngine) -> None:
    await engine.dispose()


def _install_fixed_search_path(engine: AsyncEngine, search_path: str) -> None:
    if not _is_safe_search_path(search_path):
        raise ValueError("search_path must contain lowercase SQL identifiers")

    @event.listens_for(engine.sync_engine, "connect")
    def set_search_path(dbapi_connection: Any, _connection_record: Any) -> None:
        cursor = dbapi_connection.cursor()
        paths = [f'"{part}"' for part in search_path.split(",")]
        cursor.execute("SET search_path TO " + ",".join(paths))
        cursor.close()


def _is_safe_search_path(search_path: str) -> bool:
    return all(part.isidentifier() and part.islower() for part in search_path.split(","))


@asynccontextmanager
async def session_scope(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncSession]:
    async with session_factory() as session:
        async with session.begin():
            yield session
