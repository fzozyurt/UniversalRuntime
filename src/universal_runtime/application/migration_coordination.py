from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from sqlalchemy import text

MigrationDecision = Literal["migrate", "wait", "skip"]


@dataclass(frozen=True, slots=True)
class MigrationClaim:
    decision: MigrationDecision
    application_id: str
    workspace_key: str
    environment: str
    app_version: str
    target_revision: str
    worker_id: str | None = None


class ApplicationMigrationCoordinator:
    """Atomically elect one registering worker to run an application migration."""

    def __init__(self, engine: Any) -> None:
        self._engine = engine

    async def claim(
        self,
        *,
        application_id: str,
        workspace_key: str,
        environment: str,
        app_version: str,
        target_revision: str,
        worker_id: str,
    ) -> MigrationClaim:
        async with self._engine.begin() as connection:
            result = await connection.execute(
                text(
                    """
                    INSERT INTO rt_core.application_migrations (
                        id, application_id, workspace_key, environment,
                        app_version, target_revision, worker_id, status,
                        attempt_number, created_at, updated_at
                    ) VALUES (
                        gen_random_uuid()::text, :application_id, :workspace_key, :environment,
                        :app_version, :target_revision, :worker_id, 'migrating',
                        1, NOW(), NOW()
                    )
                    ON CONFLICT (application_id, workspace_key, environment, app_version)
                    DO UPDATE SET
                        worker_id = CASE
                            WHEN rt_core.application_migrations.status IN ('fail', 'pending')
                            THEN EXCLUDED.worker_id
                            ELSE rt_core.application_migrations.worker_id
                        END,
                        target_revision = EXCLUDED.target_revision,
                        status = CASE
                            WHEN rt_core.application_migrations.status IN ('fail', 'pending')
                            THEN 'migrating'
                            ELSE rt_core.application_migrations.status
                        END,
                        attempt_number = CASE
                            WHEN rt_core.application_migrations.status IN ('fail', 'pending')
                            THEN rt_core.application_migrations.attempt_number + 1
                            ELSE rt_core.application_migrations.attempt_number
                        END,
                        error = CASE
                            WHEN rt_core.application_migrations.status IN ('fail', 'pending')
                            THEN NULL
                            ELSE rt_core.application_migrations.error
                        END,
                        updated_at = NOW()
                    RETURNING status, worker_id
                    """
                ),
                {
                    "application_id": application_id,
                    "workspace_key": workspace_key,
                    "environment": environment,
                    "app_version": app_version,
                    "target_revision": target_revision,
                    "worker_id": worker_id,
                },
            )
            row = result.mappings().one()

        status = str(row["status"])
        owner = str(row["worker_id"]) if row["worker_id"] is not None else None
        decision: MigrationDecision
        if status == "success":
            decision = "skip"
        elif status == "migrating" and owner == worker_id:
            decision = "migrate"
        else:
            decision = "wait"
        return MigrationClaim(
            decision,
            application_id,
            workspace_key,
            environment,
            app_version,
            target_revision,
            owner,
        )

    async def complete(
        self,
        claim: MigrationClaim,
        *,
        success: bool,
        error: str | None = None,
    ) -> None:
        async with self._engine.begin() as connection:
            await connection.execute(
                text(
                    """
                    UPDATE rt_core.application_migrations
                    SET status = :status, error = :error, updated_at = NOW()
                    WHERE application_id = :application_id
                      AND workspace_key = :workspace_key
                      AND environment = :environment
                      AND app_version = :app_version
                      AND worker_id = :worker_id
                      AND status = 'migrating'
                    """
                ),
                {
                    "status": "success" if success else "fail",
                    "error": error,
                    "application_id": claim.application_id,
                    "workspace_key": claim.workspace_key,
                    "environment": claim.environment,
                    "app_version": claim.app_version,
                    "worker_id": claim.worker_id,
                },
            )
