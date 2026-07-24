from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Literal

from sqlalchemy import text

from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.domain.identity import CommandId

MigrationDecision = Literal["migrate", "wait", "skip"]
MigrationStatus = Literal["pending", "migrating", "success", "fail"]


@dataclass(frozen=True, slots=True)
class MigrationClaim:
    decision: MigrationDecision
    application_id: str
    workspace_key: str
    environment: str
    app_version: str
    target_revision: str
    worker_id: str | None = None


@dataclass(frozen=True, slots=True)
class MigrationState:
    status: MigrationStatus
    worker_id: str | None
    target_revision: str
    error: str | None


class ApplicationMigrationCoordinator:
    """Coordinate migrations across simultaneously starting worker replicas."""

    def __init__(self, engine: Any, *, claim_timeout_seconds: int = 300) -> None:
        if claim_timeout_seconds < 1:
            raise ValueError("claim_timeout_seconds must be positive")
        self._engine = engine
        self._claim_timeout_seconds = claim_timeout_seconds

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
        parameters = {
            "id": str(CommandId.new()),
            "application_id": application_id,
            "workspace_key": workspace_key,
            "environment": environment,
            "app_version": app_version,
            "target_revision": target_revision,
            "worker_id": worker_id,
            "claim_timeout_seconds": self._claim_timeout_seconds,
        }
        async with self._engine.begin() as connection:
            inserted = await connection.execute(
                text(
                    """
                    INSERT INTO rt_core.application_migrations (
                        id, application_id, workspace_key, environment,
                        app_version, target_revision, worker_id, status,
                        attempt_number, created_at, updated_at
                    ) VALUES (
                        :id, :application_id, :workspace_key, :environment,
                        :app_version, :target_revision, :worker_id, 'migrating',
                        1, NOW(), NOW()
                    )
                    ON CONFLICT (application_id, workspace_key, environment, app_version)
                    DO NOTHING
                    RETURNING status, worker_id, target_revision, error
                    """
                ),
                parameters,
            )
            inserted_row = inserted.mappings().one_or_none()
            if inserted_row is not None:
                return MigrationClaim(
                    decision="migrate",
                    application_id=application_id,
                    workspace_key=workspace_key,
                    environment=environment,
                    app_version=app_version,
                    target_revision=target_revision,
                    worker_id=worker_id,
                )

            selected = await connection.execute(
                text(
                    """
                    SELECT status, worker_id, target_revision, error,
                           updated_at < NOW() - make_interval(secs => :claim_timeout_seconds)
                               AS stale
                    FROM rt_core.application_migrations
                    WHERE application_id = :application_id
                      AND workspace_key = :workspace_key
                      AND environment = :environment
                      AND app_version = :app_version
                    FOR UPDATE
                    """
                ),
                parameters,
            )
            row = selected.mappings().one()
            existing_revision = str(row["target_revision"] or "head")
            if existing_revision != target_revision:
                raise RuntimeFailure(
                    ErrorCode.APPLICATION_MIGRATION_FAILED,
                    "application artifact version has conflicting migration revisions",
                    details={
                        "application_id": application_id,
                        "app_version": app_version,
                        "expected_revision": existing_revision,
                        "requested_revision": target_revision,
                    },
                )

            status = str(row["status"])
            owner = str(row["worker_id"]) if row["worker_id"] is not None else None
            if status == "success":
                decision: MigrationDecision = "skip"
            elif status in {"fail", "pending"} or bool(row["stale"]):
                await connection.execute(
                    text(
                        """
                        UPDATE rt_core.application_migrations
                        SET worker_id = :worker_id,
                            target_revision = :target_revision,
                            status = 'migrating',
                            attempt_number = attempt_number + 1,
                            error = NULL,
                            updated_at = NOW()
                        WHERE application_id = :application_id
                          AND workspace_key = :workspace_key
                          AND environment = :environment
                          AND app_version = :app_version
                        """
                    ),
                    parameters,
                )
                owner = worker_id
                decision = "migrate"
            else:
                # This includes an RPC retry from the current owner. The retry
                # waits for the original migration instead of executing it twice.
                decision = "wait"

        return MigrationClaim(
            decision=decision,
            application_id=application_id,
            workspace_key=workspace_key,
            environment=environment,
            app_version=app_version,
            target_revision=target_revision,
            worker_id=owner,
        )

    async def get_state(self, claim: MigrationClaim) -> MigrationState:
        async with self._engine.connect() as connection:
            result = await connection.execute(
                text(
                    """
                    SELECT status, worker_id, target_revision, error
                    FROM rt_core.application_migrations
                    WHERE application_id = :application_id
                      AND workspace_key = :workspace_key
                      AND environment = :environment
                      AND app_version = :app_version
                    """
                ),
                {
                    "application_id": claim.application_id,
                    "workspace_key": claim.workspace_key,
                    "environment": claim.environment,
                    "app_version": claim.app_version,
                },
            )
            row = result.mappings().one_or_none()
        if row is None:
            raise RuntimeFailure(
                ErrorCode.APPLICATION_MIGRATION_FAILED,
                "application migration claim disappeared",
            )
        return MigrationState(
            status=str(row["status"]),  # type: ignore[arg-type]
            worker_id=str(row["worker_id"]) if row["worker_id"] is not None else None,
            target_revision=str(row["target_revision"] or "head"),
            error=str(row["error"]) if row["error"] is not None else None,
        )

    async def wait_for_completion(
        self,
        claim: MigrationClaim,
        *,
        timeout_seconds: float = 300,
        poll_interval_seconds: float = 0.5,
    ) -> MigrationState:
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        while True:
            state = await self.get_state(claim)
            if state.status in {"success", "fail"}:
                return state
            if asyncio.get_running_loop().time() >= deadline:
                raise RuntimeFailure(
                    ErrorCode.APPLICATION_MIGRATION_FAILED,
                    "timed out waiting for application migration",
                    details={
                        "application_id": claim.application_id,
                        "app_version": claim.app_version,
                        "owner_worker_id": state.worker_id,
                    },
                )
            await asyncio.sleep(poll_interval_seconds)

    async def complete(
        self,
        claim: MigrationClaim,
        *,
        success: bool,
        error: str | None = None,
    ) -> None:
        async with self._engine.begin() as connection:
            result = await connection.execute(
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
        if result.rowcount != 1:
            raise RuntimeFailure(
                ErrorCode.APPLICATION_MIGRATION_FAILED,
                "migration claim is no longer owned by this worker",
                details={"worker_id": claim.worker_id, "app_version": claim.app_version},
            )
