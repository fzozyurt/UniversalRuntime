from __future__ import annotations

from datetime import datetime

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from universal_runtime.adapters.postgres.models import WorkerLeaseRow, WorkerRow
from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.domain.identity import (
    ApplicationId,
    DeploymentId,
    LeaseId,
    RevisionId,
    RunId,
    WorkerId,
)
from universal_runtime.domain.workers import WorkerLease, WorkerRegistration, WorkerStatus


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

    @staticmethod
    def _lease_domain(lease: WorkerLeaseRow, worker: WorkerRow) -> WorkerLease:
        return WorkerLease(
            lease_id=LeaseId.parse(lease.lease_id),
            worker_id=WorkerId.parse(lease.worker_id),
            run_id=RunId.parse(lease.run_id),
            grpc_target=worker.grpc_target,
            expires_at=lease.expires_at,
        )

    async def _reclaim_expired_leases(self, session: AsyncSession, now: datetime) -> int:
        leases = (
            (
                await session.execute(
                    select(WorkerLeaseRow)
                    .where(
                        WorkerLeaseRow.acknowledged_at.is_(None),
                        WorkerLeaseRow.expires_at <= now,
                    )
                    .with_for_update(skip_locked=True)
                )
            )
            .scalars()
            .all()
        )
        reclaimed = 0
        for lease in leases:
            lease.acknowledged_at = now
            worker = (
                await session.execute(
                    select(WorkerRow)
                    .where(WorkerRow.worker_id == lease.worker_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if worker is not None:
                worker.active_executions = max(0, worker.active_executions - 1)
                if worker.status not in {
                    WorkerStatus.DRAINING.value,
                    WorkerStatus.OFFLINE.value,
                }:
                    worker.available_slots = min(
                        worker.max_concurrency,
                        worker.available_slots + 1,
                    )
                    if worker.available_slots > 0:
                        worker.status = WorkerStatus.READY.value
            reclaimed += 1
        return reclaimed

    async def upsert(self, registration: WorkerRegistration) -> WorkerRegistration:
        async with self._sessions() as session:
            async with session.begin():
                await self._reclaim_expired_leases(session, registration.last_heartbeat_at)
                row = (
                    await session.execute(
                        select(WorkerRow)
                        .where(WorkerRow.worker_id == str(registration.worker_id))
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                active_leases = int(
                    (
                        await session.execute(
                            select(func.count(WorkerLeaseRow.id)).where(
                                WorkerLeaseRow.worker_id == str(registration.worker_id),
                                WorkerLeaseRow.acknowledged_at.is_(None),
                                WorkerLeaseRow.expires_at > registration.last_heartbeat_at,
                            )
                        )
                    ).scalar_one()
                )
                effective_active = max(registration.active_executions, active_leases)
                effective_available = max(0, registration.max_concurrency - effective_active)
                status = registration.status
                if status in {WorkerStatus.DRAINING, WorkerStatus.OFFLINE}:
                    effective_available = 0
                elif effective_available == 0:
                    status = WorkerStatus.BUSY
                else:
                    status = WorkerStatus.READY

                if row is None:
                    row = WorkerRow(
                        id=str(registration.worker_id),
                        worker_id=str(registration.worker_id),
                        application_id=str(registration.application_id),
                        revision_id=str(registration.revision_id),
                        deployment_id=str(registration.deployment_id),
                        grpc_target=registration.grpc_target,
                        graph_ids=sorted(registration.graph_ids),
                        status=status.value,
                        max_concurrency=registration.max_concurrency,
                        active_executions=effective_active,
                        available_slots=effective_available,
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
                    row.status = status.value
                    row.max_concurrency = registration.max_concurrency
                    row.active_executions = effective_active
                    row.available_slots = effective_available
                    row.capabilities = registration.capabilities
                    row.last_heartbeat_at = registration.last_heartbeat_at
                    row.expires_at = registration.expires_at
                await session.flush()
                return self._domain(row)

    async def get(self, worker_id: WorkerId) -> WorkerRegistration:
        async with self._sessions() as session:
            row = (
                await session.execute(
                    select(WorkerRow).where(WorkerRow.worker_id == str(worker_id))
                )
            ).scalar_one_or_none()
            if row is None:
                raise RuntimeFailure(
                    ErrorCode.RESOURCE_NOT_FOUND,
                    f"worker not found: {worker_id}",
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
                (
                    await session.execute(
                        select(WorkerRow)
                        .where(
                            WorkerRow.deployment_id == str(deployment_id),
                            WorkerRow.status.in_(
                                [
                                    WorkerStatus.READY.value,
                                    WorkerStatus.BUSY.value,
                                ]
                            ),
                            WorkerRow.available_slots > 0,
                            WorkerRow.expires_at > now,
                            WorkerRow.graph_ids.contains([graph_id]),
                        )
                        .order_by(
                            WorkerRow.available_slots.desc(),
                            WorkerRow.active_executions.asc(),
                            WorkerRow.worker_id.asc(),
                        )
                    )
                )
                .scalars()
                .all()
            )
            return tuple(self._domain(row) for row in rows)

    async def acquire(
        self,
        deployment_id: DeploymentId,
        graph_id: str,
        run_id: RunId,
        *,
        now: datetime,
        expires_at: datetime,
    ) -> WorkerLease:
        if expires_at <= now:
            raise ValueError("worker lease expiry must follow acquisition time")
        async with self._sessions() as session:
            async with session.begin():
                await self._reclaim_expired_leases(session, now)
                existing = (
                    await session.execute(
                        select(WorkerLeaseRow, WorkerRow)
                        .join(WorkerRow, WorkerRow.worker_id == WorkerLeaseRow.worker_id)
                        .where(
                            WorkerLeaseRow.run_id == str(run_id),
                            WorkerLeaseRow.acknowledged_at.is_(None),
                            WorkerLeaseRow.expires_at > now,
                        )
                        .order_by(WorkerLeaseRow.created_at.desc())
                        .limit(1)
                    )
                ).first()
                if existing is not None:
                    return self._lease_domain(existing[0], existing[1])

                worker = (
                    await session.execute(
                        select(WorkerRow)
                        .where(
                            WorkerRow.deployment_id == str(deployment_id),
                            WorkerRow.status.in_(
                                [
                                    WorkerStatus.READY.value,
                                    WorkerStatus.BUSY.value,
                                ]
                            ),
                            WorkerRow.available_slots > 0,
                            WorkerRow.expires_at > now,
                            WorkerRow.graph_ids.contains([graph_id]),
                        )
                        .order_by(
                            WorkerRow.available_slots.desc(),
                            WorkerRow.active_executions.asc(),
                            WorkerRow.worker_id.asc(),
                        )
                        .with_for_update(skip_locked=True)
                        .limit(1)
                    )
                ).scalar_one_or_none()
                if worker is None:
                    raise RuntimeFailure(
                        ErrorCode.INFRASTRUCTURE_UNAVAILABLE,
                        "no healthy worker has capacity for the requested deployment and graph",
                        retryable=True,
                        details={
                            "deployment_id": str(deployment_id),
                            "graph_id": graph_id,
                        },
                    )

                worker.active_executions += 1
                worker.available_slots -= 1
                if worker.available_slots == 0:
                    worker.status = WorkerStatus.BUSY.value
                lease_id = LeaseId.new()
                lease = WorkerLeaseRow(
                    id=str(lease_id),
                    lease_id=str(lease_id),
                    worker_id=worker.worker_id,
                    run_id=str(run_id),
                    expires_at=expires_at,
                    acknowledged_at=None,
                )
                session.add(lease)
                await session.flush()
                return self._lease_domain(lease, worker)

    async def active_lease(self, run_id: RunId, *, now: datetime) -> WorkerLease | None:
        async with self._sessions() as session:
            result = (
                await session.execute(
                    select(WorkerLeaseRow, WorkerRow)
                    .join(WorkerRow, WorkerRow.worker_id == WorkerLeaseRow.worker_id)
                    .where(
                        WorkerLeaseRow.run_id == str(run_id),
                        WorkerLeaseRow.acknowledged_at.is_(None),
                        WorkerLeaseRow.expires_at > now,
                    )
                    .order_by(WorkerLeaseRow.created_at.desc())
                    .limit(1)
                )
            ).first()
            if result is None:
                return None
            return self._lease_domain(result[0], result[1])

    async def release(self, lease_id: LeaseId, *, now: datetime) -> None:
        async with self._sessions() as session:
            async with session.begin():
                lease = (
                    await session.execute(
                        select(WorkerLeaseRow)
                        .where(WorkerLeaseRow.lease_id == str(lease_id))
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                if lease is None or lease.acknowledged_at is not None:
                    return
                lease.acknowledged_at = now
                worker = (
                    await session.execute(
                        select(WorkerRow)
                        .where(WorkerRow.worker_id == lease.worker_id)
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                if worker is None:
                    return
                worker.active_executions = max(0, worker.active_executions - 1)
                if worker.status not in {
                    WorkerStatus.DRAINING.value,
                    WorkerStatus.OFFLINE.value,
                }:
                    worker.available_slots = min(
                        worker.max_concurrency,
                        worker.available_slots + 1,
                    )
                    if worker.available_slots > 0:
                        worker.status = WorkerStatus.READY.value

    async def mark_draining(self, worker_id: WorkerId, *, now: datetime) -> None:
        async with self._sessions() as session:
            async with session.begin():
                row = (
                    await session.execute(
                        select(WorkerRow)
                        .where(WorkerRow.worker_id == str(worker_id))
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                if row is None:
                    raise RuntimeFailure(
                        ErrorCode.RESOURCE_NOT_FOUND,
                        f"worker not found: {worker_id}",
                    )
                row.status = WorkerStatus.DRAINING.value
                row.available_slots = 0
                row.last_heartbeat_at = now

    async def expire(self, *, now: datetime) -> int:
        async with self._sessions() as session:
            async with session.begin():
                reclaimed = await self._reclaim_expired_leases(session, now)
                result = await session.execute(
                    update(WorkerRow)
                    .where(
                        WorkerRow.expires_at <= now,
                        WorkerRow.status != WorkerStatus.OFFLINE.value,
                    )
                    .values(status=WorkerStatus.OFFLINE.value, available_slots=0)
                )
                return reclaimed + int(getattr(result, "rowcount", 0) or 0)
