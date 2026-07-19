from __future__ import annotations

import asyncio
from logging.config import fileConfig
from typing import Any

from alembic import context
from sqlalchemy import pool, text
from sqlalchemy.ext.asyncio import async_engine_from_config

from universal_runtime.adapters.postgres.locks import migration_lock_key
from universal_runtime.adapters.postgres.models import PlatformBase
from universal_runtime.adapters.postgres.schema import DEFAULT_SCHEMAS

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = PlatformBase.metadata


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_schemas=True,
        version_table_schema=DEFAULT_SCHEMAS.core,
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Any) -> None:
    x_args = context.get_x_argument(as_dictionary=True)
    connection.execute(
        text("SELECT pg_advisory_xact_lock(:lock_key)"),
        {
            "lock_key": migration_lock_key(
                x_args.get("application_id", "platform"),
                x_args.get("environment", "default"),
                "platform",
            )
        },
    )
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        include_schemas=True,
        version_table_schema=DEFAULT_SCHEMAS.core,
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


def run_migrations_online() -> None:
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
