from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from alembic import command
from alembic.config import Config
from sqlalchemy import text

from universal_runtime.adapters.postgres.locks import advisory_migration_lock
from universal_runtime.adapters.postgres.schema import DEFAULT_SCHEMAS
from universal_runtime.domain.errors import ErrorCode, RuntimeFailure

_RUNTIME_ALEMBIC = Path(__file__).with_name("alembic")


@dataclass(frozen=True, slots=True)
class ApplicationMigrationBundle:
    """Application-owned revisions executed by the Runtime Alembic environment.

    Applications ship only revision files. Runtime owns env.py, script.py.mako,
    connection selection, schema targeting, version-table placement and locking.
    """

    versions_path: Path
    workspace_key: str
    application_key: str
    target_metadata: Any | None = None

    def schema_name(self, environment: str) -> str:
        return DEFAULT_SCHEMAS.application(
            self.workspace_key,
            self.application_key,
            environment,
        )


class AlembicApplicationMigrationRunner:
    def __init__(self, engine: Any) -> None:
        self.engine = engine

    async def upgrade(
        self,
        *,
        bundle: ApplicationMigrationBundle,
        application_id: str,
        environment: str,
        revision: str = "head",
    ) -> None:
        versions_path = bundle.versions_path.resolve()
        if not versions_path.is_dir():
            raise RuntimeFailure(
                ErrorCode.APPLICATION_MIGRATION_FAILED,
                f"application migration versions directory does not exist: {versions_path}",
            )

        schema = bundle.schema_name(environment)
        config = Config()
        config.set_main_option("script_location", str(_RUNTIME_ALEMBIC))
        config.set_main_option("version_locations", str(versions_path))
        config.set_main_option("version_path_separator", "os")
        config.set_main_option("application_schema", schema)
        config.set_main_option("version_table_schema", schema)
        config.set_main_option("version_table", "alembic_version")
        config.attributes["target_metadata"] = bundle.target_metadata

        try:
            # The advisory lock and Alembic upgrade must use the same database
            # connection. A transaction-scoped lock on one connection cannot
            # protect migration work performed on another pooled connection.
            async with advisory_migration_lock(
                self.engine, application_id, environment, "application"
            ) as connection:
                await connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))

                def _upgrade(sync_connection: Any) -> None:
                    config.attributes["connection"] = sync_connection
                    command.upgrade(config, revision)

                await connection.run_sync(_upgrade)
        except RuntimeFailure:
            raise
        except Exception as exc:
            raise RuntimeFailure(
                ErrorCode.APPLICATION_MIGRATION_FAILED,
                "application migration failed",
                details={
                    "application_id": application_id,
                    "environment": environment,
                    "schema": schema,
                    "revision": revision,
                },
            ) from exc
