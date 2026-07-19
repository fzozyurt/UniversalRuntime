from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from typing import cast

from fastapi import APIRouter, Body

from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.domain.identity import (
    ApplicationId,
    DeploymentId,
    RevisionId,
    WorkerId,
)
from universal_runtime.domain.primitives.json_types import JsonObject
from universal_runtime.domain.workers import WorkerRegistration, WorkerStatus
from universal_runtime.ports.workers import WorkerRegistry


def create_worker_registry_router(registry: WorkerRegistry) -> APIRouter:
    router = APIRouter(tags=["internal-workers"])

    @router.post("/internal/workers/register")
    async def register_worker(payload: JsonObject = Body(...)) -> JsonObject:
        worker_id = str(payload.get("worker_id", ""))
        grpc_target = str(payload.get("grpc_target") or payload.get("target") or "")
        graph_ids_raw = payload.get("graph_ids")
        if not worker_id or not grpc_target or not isinstance(graph_ids_raw, list):
            raise RuntimeFailure(
                ErrorCode.INVALID_EXECUTION_INPUT,
                "worker_id, grpc_target and graph_ids are required",
            )
        graph_ids = frozenset(str(item) for item in graph_ids_raw if str(item))
        now = datetime.now(UTC)
        ttl_seconds = int(os.environ.get("UR_WORKER_REGISTRATION_TTL_SECONDS", "45"))
        registration = WorkerRegistration(
            worker_id=WorkerId.parse(worker_id),
            application_id=ApplicationId.parse(str(payload.get("application_id", "default"))),
            revision_id=RevisionId.parse(str(payload.get("revision_id", "active"))),
            deployment_id=DeploymentId.parse(str(payload.get("deployment_id", "local"))),
            grpc_target=grpc_target,
            graph_ids=graph_ids,
            max_concurrency=int(payload.get("max_concurrency", 1)),
            active_executions=int(payload.get("active_executions", 0)),
            available_slots=int(payload.get("available_slots", 1)),
            status=WorkerStatus(str(payload.get("status", WorkerStatus.READY.value))),
            capabilities=cast(JsonObject, payload.get("manifests") or payload.get("capabilities") or {}),
            last_heartbeat_at=now,
            expires_at=now + timedelta(seconds=ttl_seconds),
        )
        stored = await registry.upsert(registration)
        return {
            "registered": True,
            "worker_id": str(stored.worker_id),
            "deployment_id": str(stored.deployment_id),
            "graph_ids": sorted(stored.graph_ids),
            "target": stored.grpc_target,
            "expires_at": stored.expires_at.isoformat(),
        }

    @router.get("/internal/workers")
    async def list_workers(
        deployment_id: str,
        graph_id: str,
    ) -> list[JsonObject]:
        now = datetime.now(UTC)
        candidates = await registry.candidates(
            DeploymentId.parse(deployment_id), graph_id, now=now
        )
        return [
            {
                "worker_id": str(item.worker_id),
                "application_id": str(item.application_id),
                "revision_id": str(item.revision_id),
                "deployment_id": str(item.deployment_id),
                "graph_ids": sorted(item.graph_ids),
                "target": item.grpc_target,
                "max_concurrency": item.max_concurrency,
                "active_executions": item.active_executions,
                "available_slots": item.available_slots,
                "status": item.status.value,
                "expires_at": item.expires_at.isoformat(),
            }
            for item in candidates
        ]

    return router
