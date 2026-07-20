from __future__ import annotations

import hashlib
import json
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from universal_runtime.domain.applications import (
    ApplicationDeploymentRegistration,
    ResolvedExecutionPlan,
)
from universal_runtime.domain.assistants import Assistant
from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.domain.execution import ExecutionTarget
from universal_runtime.domain.identity import (
    ApplicationId,
    ApplicationScope,
    AssistantId,
    DeploymentId,
    ProjectId,
    RevisionId,
    WorkspaceId,
)
from universal_runtime.domain.primitives.json_types import JsonObject
from universal_runtime.ports.control_plane import (
    ApplicationDeploymentCatalog,
    ExecutionPlanResolver,
)


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


def default_assistant_id(application_id: ApplicationId, graph_id: str) -> AssistantId:
    return AssistantId.parse(f"{application_id}:{graph_id}")


class PostgresControlPlaneCatalog(
    ApplicationDeploymentCatalog,
    ExecutionPlanResolver,
):
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        *,
        environment: str,
    ) -> None:
        self._sessions = sessions
        self._environment = environment

    async def register(
        self,
        registration: ApplicationDeploymentRegistration,
    ) -> tuple[Assistant, ...]:
        revision_descriptor: JsonObject = {
            "image_digest": registration.image_digest,
            "graphs": [
                {
                    "graph_id": graph.graph_id,
                    "entrypoint": graph.entrypoint,
                    "descriptor": graph.descriptor,
                }
                for graph in registration.graphs
            ],
            "metadata": registration.revision_metadata,
        }
        revision_hash = _digest(revision_descriptor)
        assistants: list[Assistant] = []
        async with self._sessions() as session:
            async with session.begin():
                await session.execute(
                    text("SELECT pg_advisory_xact_lock(hashtext(:key))"),
                    {
                        "key": (
                            "deployment-register:"
                            f"{registration.application_id}"
                        )
                    },
                )
                current_active = (
                    await session.execute(
                        text(
                            "SELECT active_revision_id FROM rt_core.applications "
                            "WHERE id = :id"
                        ),
                        {"id": str(registration.application_id)},
                    )
                ).scalar_one_or_none()
                activate_revision = (
                    current_active is None
                    or current_active == str(registration.revision_id)
                    or registration.activate_revision
                )
                await session.execute(
                    text(
                        "INSERT INTO rt_core.applications "
                        "(id, workspace_id, project_id, name, environment, "
                        "active_revision_id) "
                        "VALUES (:id, :workspace_id, :project_id, :name, "
                        ":environment, :revision_id) "
                        "ON CONFLICT (id) DO UPDATE SET "
                        "workspace_id = EXCLUDED.workspace_id, "
                        "project_id = EXCLUDED.project_id, "
                        "name = EXCLUDED.name, "
                        "environment = EXCLUDED.environment, "
                        "active_revision_id = CASE "
                        "WHEN :activate_revision THEN EXCLUDED.active_revision_id "
                        "ELSE rt_core.applications.active_revision_id END"
                    ),
                    {
                        "id": str(registration.application_id),
                        "workspace_id": str(registration.workspace_id),
                        "project_id": str(registration.project_id),
                        "name": registration.application_name,
                        "environment": registration.environment,
                        "revision_id": str(registration.revision_id),
                        "activate_revision": activate_revision,
                    },
                )
                await session.execute(
                    text(
                        "INSERT INTO rt_core.application_runtime_revisions "
                        "(id, application_id, image_digest, descriptor_hash, "
                        "metadata, active) "
                        "VALUES (:id, :application_id, :image_digest, "
                        ":descriptor_hash, CAST(:metadata AS jsonb), :active) "
                        "ON CONFLICT (id) DO NOTHING"
                    ),
                    {
                        "id": str(registration.revision_id),
                        "application_id": str(registration.application_id),
                        "image_digest": registration.image_digest,
                        "descriptor_hash": revision_hash,
                        "metadata": _canonical(
                            registration.revision_metadata
                        ),
                        "active": activate_revision,
                    },
                )
                revision_row = (
                    (
                        await session.execute(
                            text(
                                "SELECT application_id, image_digest, "
                                "descriptor_hash "
                                "FROM rt_core.application_runtime_revisions "
                                "WHERE id = :id"
                            ),
                            {"id": str(registration.revision_id)},
                        )
                    )
                    .mappings()
                    .one()
                )
                if (
                    revision_row["application_id"]
                    != str(registration.application_id)
                    or revision_row["image_digest"]
                    != registration.image_digest
                    or revision_row["descriptor_hash"] != revision_hash
                ):
                    raise RuntimeFailure(
                        ErrorCode.INVALID_EXECUTION_INPUT,
                        "immutable application revision conflicts with "
                        "existing revision",
                        details={
                            "revision_id": str(registration.revision_id)
                        },
                    )
                if activate_revision:
                    await session.execute(
                        text(
                            "UPDATE rt_core.application_runtime_revisions "
                            "SET active = (id = :revision_id) "
                            "WHERE application_id = :application_id"
                        ),
                        {
                            "application_id": str(
                                registration.application_id
                            ),
                            "revision_id": str(registration.revision_id),
                        },
                    )
                await session.execute(
                    text(
                        "INSERT INTO rt_core.deployments "
                        "(id, application_id, revision_id, environment, status) "
                        "VALUES (:id, :application_id, :revision_id, "
                        ":environment, 'ready') "
                        "ON CONFLICT (id) DO UPDATE SET "
                        "application_id = EXCLUDED.application_id, "
                        "revision_id = EXCLUDED.revision_id, "
                        "environment = EXCLUDED.environment, "
                        "status = EXCLUDED.status"
                    ),
                    {
                        "id": str(registration.deployment_id),
                        "application_id": str(registration.application_id),
                        "revision_id": str(registration.revision_id),
                        "environment": registration.environment,
                    },
                )
                for graph in registration.graphs:
                    graph_revision_id = (
                        f"{registration.revision_id}:{graph.graph_id}"
                    )
                    descriptor_hash = _digest(graph.descriptor)
                    await session.execute(
                        text(
                            "INSERT INTO rt_core.graph_revisions "
                            "(id, application_id, revision_id, graph_id, "
                            "entrypoint, descriptor, descriptor_hash) "
                            "VALUES (:id, :application_id, :revision_id, "
                            ":graph_id, :entrypoint, "
                            "CAST(:descriptor AS jsonb), :descriptor_hash) "
                            "ON CONFLICT (id) DO NOTHING"
                        ),
                        {
                            "id": graph_revision_id,
                            "application_id": str(
                                registration.application_id
                            ),
                            "revision_id": str(registration.revision_id),
                            "graph_id": graph.graph_id,
                            "entrypoint": graph.entrypoint,
                            "descriptor": _canonical(graph.descriptor),
                            "descriptor_hash": descriptor_hash,
                        },
                    )
                    existing_graph = (
                        await session.execute(
                            text(
                                "SELECT descriptor_hash "
                                "FROM rt_core.graph_revisions WHERE id = :id"
                            ),
                            {"id": graph_revision_id},
                        )
                    ).scalar_one()
                    if existing_graph != descriptor_hash:
                        raise RuntimeFailure(
                            ErrorCode.INVALID_EXECUTION_INPUT,
                            "immutable graph revision conflicts with "
                            "existing descriptor",
                            details={
                                "graph_revision_id": graph_revision_id
                            },
                        )
                    if activate_revision:
                        await session.execute(
                            text(
                                "INSERT INTO rt_core.graphs "
                                "(id, application_id, revision_id, graph_id, "
                                "entrypoint, descriptor) "
                                "VALUES (:id, :application_id, :revision_id, "
                                ":graph_id, :entrypoint, "
                                "CAST(:descriptor AS jsonb)) "
                                "ON CONFLICT (id) DO UPDATE SET "
                                "revision_id = EXCLUDED.revision_id, "
                                "entrypoint = EXCLUDED.entrypoint, "
                                "descriptor = EXCLUDED.descriptor"
                            ),
                            {
                                "id": (
                                    f"{registration.application_id}:"
                                    f"{graph.graph_id}"
                                ),
                                "application_id": str(
                                    registration.application_id
                                ),
                                "revision_id": str(
                                    registration.revision_id
                                ),
                                "graph_id": graph.graph_id,
                                "entrypoint": graph.entrypoint,
                                "descriptor": _canonical(graph.descriptor),
                            },
                        )
                    assistant_id = default_assistant_id(
                        registration.application_id,
                        graph.graph_id,
                    )
                    await session.execute(
                        text(
                            "INSERT INTO rt_core.assistants "
                            "(id, application_id, graph_id, active_version) "
                            "VALUES (:id, :application_id, :graph_id, 1) "
                            "ON CONFLICT (id) DO NOTHING"
                        ),
                        {
                            "id": str(assistant_id),
                            "application_id": str(
                                registration.application_id
                            ),
                            "graph_id": graph.graph_id,
                        },
                    )
                    metadata: JsonObject = {
                        "runtime.default": True,
                        "runtime.auto_registered": True,
                        "runtime.application_id": str(
                            registration.application_id
                        ),
                        "runtime.revision_id": str(registration.revision_id),
                        "runtime.deployment_id": str(
                            registration.deployment_id
                        ),
                        "name": graph.graph_id,
                    }
                    await session.execute(
                        text(
                            "INSERT INTO rt_core.assistant_versions "
                            "(id, assistant_id, version, config, context, "
                            "metadata) "
                            "VALUES (:id, :assistant_id, 1, '{}'::jsonb, "
                            "'{}'::jsonb, CAST(:metadata AS jsonb)) "
                            "ON CONFLICT (assistant_id, version) DO NOTHING"
                        ),
                        {
                            "id": f"{assistant_id}:1",
                            "assistant_id": str(assistant_id),
                            "metadata": _canonical(metadata),
                        },
                    )
                    assistant_metadata: JsonObject = {
                        key: value
                        for key, value in metadata.items()
                        if key != "name"
                    }
                    assistants.append(
                        Assistant(
                            assistant_id=assistant_id,
                            graph_id=graph.graph_id,
                            version=1,
                            name=graph.graph_id,
                            metadata=assistant_metadata,
                        )
                    )
        return tuple(assistants)

    async def resolve(
        self,
        assistant_id: AssistantId,
        *,
        version: int | None = None,
    ) -> ResolvedExecutionPlan:
        async with self._sessions() as session:
            row = (
                (
                    await session.execute(
                        text(
                            "SELECT a.application_id, a.graph_id, "
                            "av.version AS resolved_version, av.config, "
                            "av.context, av.metadata, app.workspace_id, "
                            "app.project_id, app.active_revision_id, "
                            "d.id AS deployment_id "
                            "FROM rt_core.assistants a "
                            "JOIN rt_core.assistant_versions av "
                            "ON av.assistant_id = a.id "
                            "AND av.version = COALESCE(:version, "
                            "a.active_version) "
                            "JOIN rt_core.applications app "
                            "ON app.id = a.application_id "
                            "JOIN rt_core.deployments d "
                            "ON d.application_id = app.id "
                            "AND d.revision_id = app.active_revision_id "
                            "WHERE a.id = :assistant_id "
                            "AND d.environment = :environment "
                            "AND d.status IN ('ready', 'active') "
                            "ORDER BY d.updated_at DESC LIMIT 1"
                        ),
                        {
                            "assistant_id": str(assistant_id),
                            "environment": self._environment,
                            "version": version,
                        },
                    )
                )
                .mappings()
                .first()
            )
            if row is None:
                suffix = (
                    f" version {version}" if version is not None else ""
                )
                raise RuntimeFailure(
                    ErrorCode.RESOURCE_NOT_FOUND,
                    f"assistant execution plan not found: {assistant_id}"
                    f"{suffix}",
                )
            raw_metadata = row["metadata"]
            raw_config = row["config"]
            raw_context = row["context"]
            if not isinstance(raw_metadata, dict):
                raise RuntimeFailure(
                    ErrorCode.INVALID_EXECUTION_INPUT,
                    "stored assistant metadata is not a JSON object",
                )
            if not isinstance(raw_config, dict) or not isinstance(
                raw_context,
                dict,
            ):
                raise RuntimeFailure(
                    ErrorCode.INVALID_EXECUTION_INPUT,
                    "stored assistant config/context is not a JSON object",
                )
            resolved_metadata: JsonObject = dict(raw_metadata)
            name_value = resolved_metadata.pop("name", None)
            assistant = Assistant(
                assistant_id=assistant_id,
                graph_id=str(row["graph_id"]),
                version=int(row["resolved_version"]),
                name=str(name_value) if name_value is not None else None,
                config=dict(raw_config),
                context=dict(raw_context),
                metadata=resolved_metadata,
            )
            scope = ApplicationScope(
                WorkspaceId.parse(str(row["workspace_id"])),
                ProjectId.parse(str(row["project_id"])),
                ApplicationId.parse(str(row["application_id"])),
                RevisionId.parse(str(row["active_revision_id"])),
                DeploymentId.parse(str(row["deployment_id"])),
            )
            return ResolvedExecutionPlan(
                scope=scope,
                assistant=assistant,
                target=ExecutionTarget(
                    assistant.graph_id,
                    assistant.version,
                ),
            )
