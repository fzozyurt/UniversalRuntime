from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from sqlalchemy.ext.asyncio import AsyncEngine

from universal_runtime.adapters.langgraph.adapter import LangGraphAdapter
from universal_runtime.adapters.langgraph.persistence import postgres_persistence
from universal_runtime.adapters.postgres.langgraph import managed_langgraph_persistence
from universal_runtime.adapters.postgres.schema import DEFAULT_SCHEMAS, SchemaNames


@asynccontextmanager
async def detect_and_create_postgres_adapter(
    target: Any,
    *,
    database_url: str,
    migration_engine: AsyncEngine,
    application_id: str,
    workspace_key: str,
    environment: str,
    application_key: str,
    schemas: SchemaNames = DEFAULT_SCHEMAS,
) -> AsyncIterator[LangGraphAdapter]:
    """Detect a graph and create its managed PostgreSQL runtime adapter.

    Builder/factory targets receive both upstream providers at compile time.
    Already-compiled graphs are intentionally rejected by ``LangGraphAdapter``
    because changing private persistence attributes would be unsafe.
    """
    async with managed_langgraph_persistence(
        database_url,
        migration_engine=migration_engine,
        application_id=application_id,
        environment=environment,
        workspace_key=workspace_key,
        application_key=application_key,
        schemas=schemas,
    ) as persistence:
        providers = postgres_persistence(persistence.checkpointer, persistence.store)
        yield LangGraphAdapter(target, persistence_mode="platform-managed", providers=providers)
