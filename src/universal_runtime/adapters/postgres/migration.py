from __future__ import annotations

import asyncio
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy.ext.asyncio import AsyncEngine

from universal_runtime.adapters.postgres.schema import (
    DEFAULT_SCHEMAS,
    SchemaNames,
)

_PROJECT_ROOT = Path(__file__).resolve().parents[4]
_ALEMBIC_INI = _PROJECT_ROOT / "alembic.ini"


def _alembic_upgrade(
    database_url: str,
    application_id: str,
    environment: str,
) -> None:
    config = Config(str(_ALEMBIC_INI))
    config.set_main_option(
        "script_location",
        str(_PROJECT_ROOT / "migrations"),
    )
    config.set_main_option(
        "sqlalchemy.url",
        database_url.replace("%", "%%"),
    )
    config.set_main_option("runtime.application_id", application_id)
    config.set_main_option("runtime.environment", environment)
    command.upgrade(config, "head")


async def migrate_platform(
    engine: AsyncEngine,
    *,
    application_id: str,
    environment: str,
    schemas: SchemaNames = DEFAULT_SCHEMAS,
) -> None:
    if schemas != DEFAULT_SCHEMAS:
        raise ValueError(
            "Alembic platform migrations currently require DEFAULT_SCHEMAS; "
            "custom schema prefixes are reserved for application/state schemas"
        )
    database_url = engine.url.render_as_string(hide_password=False)
    await asyncio.to_thread(
        _alembic_upgrade,
        database_url,
        application_id,
        environment,
    )
