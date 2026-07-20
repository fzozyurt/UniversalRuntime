from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

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
from universal_runtime.domain.primitives.json_types import (
    JsonObject,
    JsonValue,
)
from universal_runtime.domain.workers import WorkerRegistration, WorkerStatus
from universal_runtime.ports.control_plane import ApplicationDeploymentCatalog
from universal_runtime.ports.workers import WorkerRegistry

_TRUE_VALUES = {"1", "true", "yes", "on"}


def _invalid(field: str, expected: str) -> ValueError:
    return ValueError(f"{field} must be {expected}")


def _object(value: JsonValue, field: str) -> JsonObject:
    if not isinstance(value, dict):
        raise _invalid(field, "an object")
    return value


def _optional_object(value: JsonValue, field: str) -> JsonObject:
    if value is None:
        return {}
    return _object(value, field)


def _string(
    value: JsonValue,
    field: str,
    *,
    default: str | None = None,
) -> str:
    if value is None and default is not None:
        return default
    if not isinstance(value, str) or not value.strip():
        raise _invalid(field, "a non-empty string")
    return value.strip()


def _integer(
    value: JsonValue,
    field: str,
    *,
    default: int,
    minimum: int = 0,
) -> int:
    if value is None:
        value = default
    if isinstance(value, bool) or not isinstance(value, int):
        raise _invalid(field, "an integer")
    if value < minimum:
        raise _invalid(field, f"an integer greater than or equal to {minimum}")
    return value


def _boolean(value: JsonValue, field: str, *, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip().lower() in _TRUE_VALUES:
        return True
    if isinstance(value, str) and value.strip().lower() in {
        "0",
        "false",
        "no",
        "off",
    }:
        return False
    raise _invalid(field, "a boolean")


def _graphs(payload: JsonObject) -> tuple[GraphRegistration, ...]:
    raw_graphs = payload.get("graphs")
    if raw_graphs is not None:
        if not isinstance(raw_graphs, list):
            raise _invalid("graphs", "an array")
        result: list[GraphRegistration] = []
        for index, item in enumerate(raw_graphs):
            graph = _object(item, f"graphs[{index}]")
            result.append(
                GraphRegistration(
                    graph_id=_string(
                        graph.get("graph_id"),
                        f"graphs[{index}].graph_id",
                    ),
                    entrypoint=_string(
                        graph.get("entrypoint"),
                        f"graphs[{index}].entrypoint",
                    ),
                    descriptor=_optional_object(
                        graph.get("descriptor"),
                        f"graphs[{index}].descriptor",
                    ),
                )
            )
        if not result:
            raise _invalid("graphs", "a non-empty array")
        return tuple(result)

    raw_graph_ids = payload.get("graph_ids")
    if not isinstance(raw_graph_ids, list) or not raw_graph_ids:
        raise _invalid("graph_ids", "a non-empty array of strings")
    manifests = _optional_object(payload.get("manifests"), "manifests")
    registrations: list[GraphRegistration] = []
    for index, graph_value in enumerate(raw_graph_ids):
        graph_id = _string(graph_value, f"graph_ids[{index}]")
        descriptor_value = manifests.get(graph_id)
        registrations.append(
            GraphRegistration(
                graph_id=graph_id,
                entrypoint=graph_id,
                descriptor=_optional_object(
                    descriptor_value,
                    f"manifests.{graph_id}",
                ),
            )
        )
    return tuple(registrations)


def _json_strings(values: list[str]) -> list[JsonValue]:
    return list(values)


def create_worker_registry_router(
    registry: WorkerRegistry,
    catalog: ApplicationDeploymentCatalog,
) -> APIRouter:
    router = APIRouter(tags=["internal-workers"])

    @router.post("/internal/workers/register")
    async def register_worker(payload: JsonObject = Body(...)) -> JsonObject:
        try:
            worker_id = _string(payload.get("worker_id"), "worker_id")
            grpc_target = _string(
                payload.get("grpc_target") or payload.get("target"),
                "grpc_target",
            )
            graphs = _graphs(payload)
            application_id = ApplicationId.parse(
                _string(
                    payload.get("application_id"),
                    "application_id",
                    default="default",
                )
            )
            revision_id = RevisionId.parse(
                _string(
                    payload.get("revision_id"),
                    "revision_id",
                    default="active",
                )
            )
            deployment_id = DeploymentId.parse(
                _string(
                    payload.get("deployment_id"),
                    "deployment_id",
                    default="local",
                )
            )
            deployment = ApplicationDeploymentRegistration(
                workspace_id=WorkspaceId.parse(
                    _string(
                        payload.get("workspace_id"),
                        "workspace_id",
                        default="default",
                    )
                ),
                project_id=ProjectId.parse(
                    _string(
                        payload.get("project_id"),
                        "project_id",
                        default="default",
                    )
                ),
                application_id=application_id,
                application_name=_string(
                    payload.get("application_name")
                    or payload.get("application_id"),
                    "application_name",
                    default="default",
                ),
                revision_id=revision_id,
                deployment_id=deployment_id,
                environment=_string(
                    payload.get("environment"),
                    "environment",
                    default="local",
                ),
                image_digest=_string(
                    payload.get("image_digest"),
                    "image_digest",
                    default=f"unresolved:{revision_id}",
                ),
                graphs=graphs,
                revision_metadata=_optional_object(
                    payload.get("revision_metadata"),
                    "revision_metadata",
                ),
                activate_revision=_boolean(
                    payload.get("activate_revision"),
                    "activate_revision",
                ),
            )
            graph_ids = frozenset(graph.graph_id for graph in graphs)
            now = datetime.now(UTC)
            ttl_seconds = max(
                1,
                int(
                    os.environ.get(
                        "UR_WORKER_REGISTRATION_TTL_SECONDS",
                        "45",
                    )
                ),
            )
            status_value = _string(
                payload.get("status"),
                "status",
                default=WorkerStatus.READY.value,
            )
            registration = WorkerRegistration(
                worker_id=WorkerId.parse(worker_id),
                application_id=application_id,
                revision_id=revision_id,
                deployment_id=deployment_id,
                grpc_target=grpc_target,
                graph_ids=graph_ids,
                max_concurrency=_integer(
                    payload.get("max_concurrency"),
                    "max_concurrency",
                    default=1,
                    minimum=1,
                ),
                active_executions=_integer(
                    payload.get("active_executions"),
                    "active_executions",
                    default=0,
                ),
                available_slots=_integer(
                    payload.get("available_slots"),
                    "available_slots",
                    default=1,
                ),
                status=WorkerStatus(status_value),
                capabilities=_optional_object(
                    payload.get("manifests")
                    or payload.get("capabilities"),
                    "capabilities",
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
        response: JsonObject = {
            "registered": True,
            "worker_id": str(stored.worker_id),
            "application_id": str(stored.application_id),
            "revision_id": str(stored.revision_id),
            "deployment_id": str(stored.deployment_id),
            "graph_ids": _json_strings(sorted(stored.graph_ids)),
            "default_assistant_ids": _json_strings(
                [str(assistant.assistant_id) for assistant in assistants]
            ),
            "target": stored.grpc_target,
            "expires_at": stored.expires_at.isoformat(),
        }
        return response

    @router.get("/internal/workers")
    async def list_workers(
        deployment_id: str,
        graph_id: str,
    ) -> list[JsonObject]:
        now = datetime.now(UTC)
        candidates = await registry.candidates(
            DeploymentId.parse(deployment_id),
            graph_id,
            now=now,
        )
        result: list[JsonObject] = []
        for item in candidates:
            result.append(
                {
                    "worker_id": str(item.worker_id),
                    "application_id": str(item.application_id),
                    "revision_id": str(item.revision_id),
                    "deployment_id": str(item.deployment_id),
                    "graph_ids": _json_strings(sorted(item.graph_ids)),
                    "target": item.grpc_target,
                    "max_concurrency": item.max_concurrency,
                    "active_executions": item.active_executions,
                    "available_slots": item.available_slots,
                    "status": item.status.value,
                    "expires_at": item.expires_at.isoformat(),
                }
            )
        return result

    return router
