from __future__ import annotations

from datetime import datetime

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from universal_runtime.adapters.postgres.models import WorkerRow
from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.domain.identity import (
    ApplicationId,
    DeploymentId,
    RevisionId,
    WorkerId,
)
from universal_runtime.domain.workers import WorkerRegistration, WorkerStatus


class PostgresWorkerRegistry:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    @staticmethod
    def _domain(row: WorkerRow) -> WorkerRegistration:
        return WorkerRegistration(
            worker_id=WorkerId.parse(row.worker_id),
            application_id=ApplicationId.parse(row.application_id),
            revision_id=RevisionId.parse(row.revision_id),
            deployment_id=DeploymentId.parse(row.deployment_id),
            grpc_target=row.grpc_target,
            graph_ids=frozenset(row.graph_ids),
            max_concurrency=row.max_concurrency,
            active_executions=row.active_executions,
            available_slots=row.available_slots,
            status=WorkerStatus(row.status),
            capabilities=row.capabilities,
            last_heartbeat_at=row.last_heartbeat_at,
            expires_at=row.expires_at,
        )

    async def upsert(self, registration: WorkerRegistration) -> WorkerRegistration:
        async with self._sessions() as session:
            async with session.begin():
                row = (
                    await session.execute(
                        select(WorkerRow).where(
                            WorkerRow.worker_id == str(registration.worker_id)
                        )
                    )
                ).scalar_one_or_none()
                if row is None:
                    row = WorkerRow(
                        id=str(registration.worker_id),
                        worker_id=str(registration.worker_id),
                        application_id=str(registration.application_id),
                        revision_id=str(registration.revision_id),
                        deployment_id=str(registration.deployment_id),
                        grpc_target=registration.grpc_target,
                        graph_ids=sorted(registration.graph_ids),
                        status=registration.status.value,
                        max_concurrency=registration.max_concurrency,
                        active_executions=registration.active_executions,
                        available_slots=registration.available_slots,
                        capabilities=registration.capabilities,
                        last_heartbeat_at=registration.last_heartbeat_at,
                        expires_at=registration.expires_at,
                    )
                    session.add(row)
                else:
                    row.application_id = str(registration.application_id)
                    row.revision_id = str(registration.revision_id)
                    row.deployment_id = str(registration.deployment_id)
                    row.grpc_target = registration.grpc_target
                    row.graph_ids = sorted(registration.graph_ids)
                    row.status = registration.status.value
                    row.max_concurrency = registration.max_concurrency
                    row.active_executions = registration.active_executions
                    row.available_slots = registration.available_slots
                    row.capabilities = registration.capabilities
                    row.last_heartbeat_at = registration.last_heartbeat_at
                    row.expires_at = registration.expires_at
        return registration

    async def get(self, worker_id: WorkerId) -> WorkerRegistration:
        async with self._sessions() as session:
            row = (
                await session.execute(
                    select(WorkerRow).where(WorkerRow.worker_id == str(worker_id))
                )
            ).scalar_one_or_none()
            if row is None:
                raise RuntimeFailure(
                    ErrorCode.RESOURCE_NOT_FOUND, f"worker not found: {worker_id}"
                )
            return self._domain(row)

    async def candidates(
        self,
        deployment_id: DeploymentId,
        graph_id: str,
        *,
        now: datetime,
    ) -> tuple[WorkerRegistration, ...]:
        async with self._sessions() as session:
            rows = (
                await session.execute(
                    select(WorkerRow)
                    .where(
                        WorkerRow.deployment_id == str(deployment_id),
                        WorkerRow.status.in_([
                            WorkerStatus.READY.value,
                            WorkerStatus.BUSY.value,
                        ]),
                        WorkerRow.available_slots > 0,
                        WorkerRow.expires_at > now,
                    )
                    .order_by(
                        WorkerRow.available_slots.desc(),
                        WorkerRow.active_executions.asc(),
                        WorkerRow.worker_id.asc(),
                    )
                )
            ).scalars().all()
            return tuple(
                self._domain(row) for row in rows if graph_id in set(row.graph_ids)
            )

    async def mark_draining(self, worker_id: WorkerId, *, now: datetime) -> None:
        async with self._sessions() as session:
            async with session.begin():
                row = (
                    await session.execute(
                        select(WorkerRow).where(WorkerRow.worker_id == str(worker_id))
                    )
                ).scalar_one_or_none()
                if row is None:
                    raise RuntimeFailure(
                        ErrorCode.RESOURCE_NOT_FOUND, f"worker not found: {worker_id}"
                    )
                row.status = WorkerStatus.DRAINING.value
                row.available_slots = 0
                row.last_heartbeat_at = now

    async def expire(self, *, now: datetime) -> int:
        async with self._sessions() as session:
            async with session.begin():
                result = await session.execute(
                    delete(WorkerRow).where(WorkerRow.expires_at <= now)
                )
                return int(result.rowcount or 0)
