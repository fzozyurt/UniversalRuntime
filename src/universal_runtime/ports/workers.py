from __future__ import annotations

from datetime import datetime
from typing import Protocol

from universal_runtime.domain.identity import DeploymentId, WorkerId
from universal_runtime.domain.workers import WorkerRegistration


class WorkerRegistry(Protocol):
    async def upsert(self, registration: WorkerRegistration) -> WorkerRegistration: ...

    async def get(self, worker_id: WorkerId) -> WorkerRegistration: ...

    async def candidates(
        self,
        deployment_id: DeploymentId,
        graph_id: str,
        *,
        now: datetime,
    ) -> tuple[WorkerRegistration, ...]: ...

    async def mark_draining(self, worker_id: WorkerId, *, now: datetime) -> None: ...

    async def expire(self, *, now: datetime) -> int: ...
