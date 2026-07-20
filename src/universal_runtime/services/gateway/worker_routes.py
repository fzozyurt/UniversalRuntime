from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from typing import cast

from fastapi import APIRouter, Body

from universal_runtime.domain.applications import (
    ApplicationDeploymentRegistration,
    GraphRegistration,
)
from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.domain.identity import (
    ApplicationId,
    DeploymentId,
    ProjectId,
    RevisionId,
    WorkerId,
    WorkspaceId,
)
from universal_runtime.domain.primitives.json_types import JsonObject
from universal_runtime.domain.workers import WorkerRegistration, WorkerStatus
from universal_runtime.ports.control_plane import ApplicationDeploymentCatalog
from universal_runtime.ports.workers import WorkerRegistry


def _graphs(payload: JsonObject) -> tuple[GraphRegistration, ...]:
    raw_graphs = payload.get("graphs")
    if isinstance(raw_graphs, list):
        result: list[GraphRegistration] = []
        for item in raw_graphs:
            if not isinstance(item, dict):
                raise ValueError("each graph registration must be an object")
            descriptor = item.get("descriptor")
            if not isinstance(descriptor, dict):
                descriptor = {}
            result.append(
                GraphRegistration(
                    graph_id=str(item.get("graph_id", "")),
                    entrypoint=str(item.get("entrypoint", "")),
                    descriptor=cast(JsonObject, descriptor),
                )
            )
        return tuple(result)
    graph_ids = payload.get("graph_ids")
    if not isinstance(graph_ids, list):
        raise ValueError("graph_ids or graphs is required")
    manifests = payload.get("manifests")
    manifest_map = manifests if isinstance(manifests, dict) else {}
    return tuple(
        GraphRegistration(
            graph_id=str(graph_id),
            entrypoint=str(graph_id),
            descriptor=cast(
                JsonObject,
                manifest_map.get(str(graph_id), {})
                if isinstance(manifest_map.get(str(graph_id), {}), dict)
                else {},
            ),
        )
        for graph_id in graph_ids
        if str(graph_id)
    )


def _boolean(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def create_worker_registry_router(
    registry: WorkerRegistry,
    catalog: ApplicationDeploymentCatalog,
) -> APIRouter:
    router = APIRouter(tags=["internal-workers"])

    @router.post("/internal/workers/register")
    async def register_worker(payload: JsonObject = Body(...)) -> JsonObject:
        worker_id = str(payload.get("worker_id", ""))
        grpc_target = str(payload.get("grpc_target") or payload.get("target") or "")
        if not worker_id or not grpc_target:
            raise RuntimeFailure(
                ErrorCode.INVALID_EXECUTION_INPUT,
                "worker_id and grpc_target are required",
            )
        try:
            graphs = _graphs(payload)
            application_id = ApplicationId.parse(str(payload.get("application_id", "default")))
            revision_id = RevisionId.parse(str(payload.get("revision_id", "active")))
            deployment_id = DeploymentId.parse(str(payload.get("deployment_id", "local")))
            deployment = ApplicationDeploymentRegistration(
                workspace_id=WorkspaceId.parse(str(payload.get("workspace_id", "default"))),
                project_id=ProjectId.parse(str(payload.get("project_id", "default"))),
                application_id=application_id,
                application_name=str(
                    payload.get("application_name") or payload.get("application_id") or "default"
                ),
                revision_id=revision_id,
                deployment_id=deployment_id,
                environment=str(payload.get("environment", "local")),
                image_digest=str(payload.get("image_digest") or f"unresolved:{revision_id}"),
                graphs=graphs,
                revision_metadata=cast(
                    JsonObject,
                    payload.get("revision_metadata")
                    if isinstance(payload.get("revision_metadata"), dict)
                    else {},
                ),
                activate_revision=_boolean(payload.get("activate_revision", False)),
            )
            graph_ids = frozenset(graph.graph_id for graph in graphs)
            now = datetime.now(UTC)
            ttl_seconds = int(os.environ.get("UR_WORKER_REGISTRATION_TTL_SECONDS", "45"))
            registration = WorkerRegistration(
                worker_id=WorkerId.parse(worker_id),
                application_id=application_id,
                revision_id=revision_id,
                deployment_id=deployment_id,
                grpc_target=grpc_target,
                graph_ids=graph_ids,
                max_concurrency=int(payload.get("max_concurrency", 1)),
                active_executions=int(payload.get("active_executions", 0)),
                available_slots=int(payload.get("available_slots", 1)),
                status=WorkerStatus(str(payload.get("status", WorkerStatus.READY.value))),
                capabilities=cast(
                    JsonObject,
                    payload.get("manifests") or payload.get("capabilities") or {},
                ),
                last_heartbeat_at=now,
                expires_at=now + timedelta(seconds=ttl_seconds),
            )
        except (TypeError, ValueError) as exc:
            raise RuntimeFailure(
                ErrorCode.INVALID_EXECUTION_INPUT,
                f"invalid worker registration: {exc}",
            ) from exc

        assistants = await catalog.register(deployment)
        stored = await registry.upsert(registration)
        return {
            "registered": True,
            "worker_id": str(stored.worker_id),
            "application_id": str(stored.application_id),
            "revision_id": str(stored.revision_id),
            "deployment_id": str(stored.deployment_id),
            "graph_ids": sorted(stored.graph_ids),
            "default_assistant_ids": [str(assistant.assistant_id) for assistant in assistants],
            "target": stored.grpc_target,
            "expires_at": stored.expires_at.isoformat(),
        }

    @router.get("/internal/workers")
    async def list_workers(
        deployment_id: str,
        graph_id: str,
    ) -> list[JsonObject]:
        now = datetime.now(UTC)
        candidates = await registry.candidates(DeploymentId.parse(deployment_id), graph_id, now=now)
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
