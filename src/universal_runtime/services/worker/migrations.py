from __future__ import annotations

import importlib
import os
from pathlib import Path
from typing import Any, Awaitable, Callable

from universal_runtime.adapters.fastapi.migration_runner import (
    AlembicApplicationMigrationRunner,
    ApplicationMigrationBundle,
)

MigrationHandler = Callable[[Any], Awaitable[None]]


def create_application_migration_handler(engine: Any) -> MigrationHandler:
    versions_value = os.environ.get("UR_APPLICATION_MIGRATIONS_PATH", "").strip()
    versions_path = Path(versions_value) if versions_value else None
    target_metadata = _load_target_metadata()
    runner = AlembicApplicationMigrationRunner(engine)

    async def migrate(request: Any) -> None:
        application_id = os.environ.get("UR_APPLICATION_ID", "default")
        workspace_key = os.environ.get("UR_WORKSPACE_KEY", "default")
        environment = os.environ.get("UR_KAFKA_ENVIRONMENT", "local")
        app_version = os.environ.get("ARTIFACT_VERSION", "development")

        expected = {
            "application_id": application_id,
            "workspace_key": workspace_key,
            "environment": environment,
            "app_version": app_version,
        }
        actual = {
            "application_id": request.application_id,
            "workspace_key": request.workspace_key,
            "environment": request.environment,
            "app_version": request.app_version,
        }
        mismatches = [name for name, value in expected.items() if actual[name] != value]
        if mismatches:
            details = ", ".join(
                f"{name}={actual[name]!r} expected={expected[name]!r}" for name in mismatches
            )
            raise RuntimeError(f"migration request does not match worker artifact: {details}")

        if versions_path is None:
            return

        bundle = ApplicationMigrationBundle(
            versions_path=versions_path,
            workspace_key=workspace_key,
            application_key=os.environ.get("UR_APPLICATION_KEY", application_id),
            target_metadata=target_metadata,
        )
        await runner.upgrade(
            bundle=bundle,
            application_id=application_id,
            environment=environment,
            revision=os.environ.get("UR_APPLICATION_MIGRATION_REVISION", "head"),
        )

    return migrate


def _load_target_metadata() -> Any | None:
    entrypoint = os.environ.get("UR_APPLICATION_MIGRATION_METADATA", "").strip()
    if not entrypoint:
        return None
    if ":" not in entrypoint:
        raise RuntimeError("UR_APPLICATION_MIGRATION_METADATA must use module:attribute")
    module_name, attribute = entrypoint.split(":", 1)
    value = getattr(importlib.import_module(module_name), attribute)
    return getattr(value, "metadata", value)
