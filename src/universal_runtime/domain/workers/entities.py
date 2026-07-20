from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from universal_runtime.domain.identity import (
    ApplicationId,
    DeploymentId,
    LeaseId,
    RevisionId,
    RunId,
    WorkerId,
)
from universal_runtime.domain.primitives.json_types import JsonObject


class WorkerStatus(StrEnum):
    READY = "ready"
    BUSY = "busy"
    DRAINING = "draining"
    OFFLINE = "offline"


@dataclass(frozen=True, slots=True)
class WorkerRegistration:
    worker_id: WorkerId
    application_id: ApplicationId
    revision_id: RevisionId
    deployment_id: DeploymentId
    grpc_target: str
    graph_ids: frozenset[str]
    max_concurrency: int
    active_executions: int
    available_slots: int
    status: WorkerStatus
    capabilities: JsonObject
    last_heartbeat_at: datetime
    expires_at: datetime

    def __post_init__(self) -> None:
        if not self.grpc_target.strip():
            raise ValueError("worker grpc_target must not be empty")
        if not self.graph_ids:
            raise ValueError("worker must advertise at least one graph")
        if self.max_concurrency < 1:
            raise ValueError("worker max_concurrency must be positive")
        if self.active_executions < 0 or self.available_slots < 0:
            raise ValueError("worker capacity counters must not be negative")
        if self.active_executions + self.available_slots > self.max_concurrency:
            raise ValueError("worker capacity counters exceed max_concurrency")
        if self.expires_at <= self.last_heartbeat_at:
            raise ValueError("worker registration expiry must follow heartbeat time")

    def supports(self, deployment_id: DeploymentId, graph_id: str, now: datetime) -> bool:
        return (
            self.deployment_id == deployment_id
            and graph_id in self.graph_ids
            and self.status in {WorkerStatus.READY, WorkerStatus.BUSY}
            and self.available_slots > 0
            and self.expires_at > now
        )


@dataclass(frozen=True, slots=True)
class WorkerLease:
    lease_id: LeaseId
    worker_id: WorkerId
    run_id: RunId
    grpc_target: str
    expires_at: datetime

    def __post_init__(self) -> None:
        if not self.grpc_target.strip():
            raise ValueError("worker lease grpc_target must not be empty")

    def is_active(self, now: datetime) -> bool:
        return self.expires_at > now
