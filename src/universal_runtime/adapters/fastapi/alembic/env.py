from __future__ import annotations

from logging.config import fileConfig
from typing import Any

from alembic import context
from sqlalchemy import engine_from_config, pool, text

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)


def _target_metadata() -> Any:
    return config.attributes.get("target_metadata")


def _configure(connection: Any | None = None) -> None:
    options: dict[str, Any] = {
        "target_metadata": _target_metadata(),
        "include_schemas": True,
        "compare_type": True,
        "compare_server_default": True,
        "version_table": config.get_main_option("version_table", "alembic_version"),
        "version_table_schema": config.get_main_option("version_table_schema") or None,
    }
    if connection is None:
        options.update(
            url=config.get_main_option("sqlalchemy.url"),
            literal_binds=True,
            dialect_opts={"paramstyle": "named"},
        )
    else:
        schema = config.get_main_option("application_schema")
        if schema:
            connection.execute(text(f'SET LOCAL search_path TO "{schema}", public'))
        options["connection"] = connection
    context.configure(**options)


def run_migrations_offline() -> None:
    _configure()
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connection = config.attributes.get("connection")
    if connection is not None:
        _configure(connection)
        with context.begin_transaction():
            context.run_migrations()
        return

    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as created_connection:
        _configure(created_connection)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
