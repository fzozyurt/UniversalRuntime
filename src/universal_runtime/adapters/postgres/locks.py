from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine


def migration_lock_key(application_id: str, environment: str, category: str) -> int:
    material = f"{application_id}{environment}{category}".encode()
    unsigned = int.from_bytes(hashlib.sha256(material).digest()[:8], "big", signed=False)
    return unsigned - (1 << 64) if unsigned >= (1 << 63) else unsigned


@asynccontextmanager
async def advisory_migration_lock(
    engine: AsyncEngine,
    application_id: str,
    environment: str,
    category: str,
) -> AsyncIterator[AsyncConnection]:
    async with engine.begin() as connection:
        await connection.execute(
            text("SELECT pg_advisory_xact_lock(:lock_key)"),
            {"lock_key": migration_lock_key(application_id, environment, category)},
        )
        yield connection


@asynccontextmanager
async def advisory_migration_session_lock(
    engine: AsyncEngine,
    application_id: str,
    environment: str,
    category: str,
) -> AsyncIterator[AsyncConnection]:
    """Hold a session advisory lock without keeping a transaction open.

    Upstream LangGraph setup uses ``CREATE INDEX CONCURRENTLY``. That command
    cannot run while another transaction is left open, so framework setup needs
    a session-level lock rather than ``pg_advisory_xact_lock``.
    """
    async with engine.connect() as connection:
        key = migration_lock_key(application_id, environment, category)
        await connection.execute(text("SELECT pg_advisory_lock(:lock_key)"), {"lock_key": key})
        await connection.commit()
        try:
            yield connection
        finally:
            await connection.execute(
                text("SELECT pg_advisory_unlock(:lock_key)"), {"lock_key": key}
            )
            await connection.commit()
