from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

from universal_runtime.domain.execution import (
    ExecutionRequest,
    ExecutionTarget,
    QueuePriority,
    RunCommand,
)
from universal_runtime.domain.identity import (
    ApplicationId,
    ApplicationScope,
    AssistantId,
    AttemptId,
    CommandId,
    DeploymentId,
    ExecutionIdentity,
    ProjectId,
    RevisionId,
    RunId,
    ThreadId,
    WorkspaceId,
)
from universal_runtime.domain.primitives.json_types import JsonObject


def run_command_to_document(command: RunCommand) -> JsonObject:
    identity = command.identity
    request = command.request
    return {
        "command_id": str(command.command_id),
        "identity": {
            "workspace_id": str(identity.workspace_id),
            "project_id": str(identity.project_id),
            "application_id": str(identity.application_id),
            "revision_id": str(identity.revision_id),
            "deployment_id": str(identity.deployment_id),
            "assistant_id": str(identity.assistant_id),
            "thread_id": str(identity.thread_id) if identity.thread_id else None,
            "run_id": str(identity.run_id),
            "attempt_id": str(identity.attempt_id),
        },
        "request": {
            "target": {
                "graph_id": request.target.graph_id,
                "assistant_version": request.target.assistant_version,
            },
            "input": request.input,
            "command": request.command,
            "config": request.config,
            "context": request.context,
            "metadata": request.metadata,
            "stream_modes": list(request.stream_modes),
            "stream_subgraphs": request.stream_subgraphs,
            "priority": int(request.priority),
            "timeout_seconds": request.timeout_seconds,
            "checkpoint_namespace": request.checkpoint_namespace,
            "checkpoint_id": request.checkpoint_id,
        },
        "priority": int(command.priority),
        "available_at": command.available_at.isoformat(),
        "created_at": command.created_at.isoformat(),
    }


def run_command_from_document(document: JsonObject) -> RunCommand:
    raw_identity = cast(JsonObject, document["identity"])
    identity = ExecutionIdentity(
        ApplicationScope(
            WorkspaceId.parse(str(raw_identity["workspace_id"])),
            ProjectId.parse(str(raw_identity["project_id"])),
            ApplicationId.parse(str(raw_identity["application_id"])),
            RevisionId.parse(str(raw_identity["revision_id"])),
            DeploymentId.parse(str(raw_identity["deployment_id"])),
        ),
        AssistantId.parse(str(raw_identity["assistant_id"])),
        RunId.parse(str(raw_identity["run_id"])),
        AttemptId.parse(str(raw_identity["attempt_id"])),
        (
            ThreadId.parse(str(raw_identity["thread_id"]))
            if raw_identity.get("thread_id")
            else None
        ),
    )
    raw_request = cast(JsonObject, document["request"])
    raw_target = cast(JsonObject, raw_request.get("target") or {})
    request = ExecutionRequest(
        identity=identity,
        target=ExecutionTarget(
            str(raw_target.get("graph_id") or raw_identity["assistant_id"]),
            int(raw_target.get("assistant_version") or 1),
        ),
        input=raw_request.get("input"),
        command=raw_request.get("command"),
        config=cast(JsonObject, raw_request.get("config") or {}),
        context=cast(JsonObject, raw_request.get("context") or {}),
        metadata=cast(JsonObject, raw_request.get("metadata") or {}),
        stream_modes=tuple(str(item) for item in raw_request.get("stream_modes") or ["values"]),
        stream_subgraphs=bool(raw_request.get("stream_subgraphs", False)),
        priority=QueuePriority(int(raw_request.get("priority") or QueuePriority.INTERACTIVE)),
        timeout_seconds=int(raw_request.get("timeout_seconds") or 1800),
        checkpoint_namespace=str(raw_request.get("checkpoint_namespace") or ""),
        checkpoint_id=(
            str(raw_request["checkpoint_id"])
            if raw_request.get("checkpoint_id") is not None
            else None
        ),
    )
    available_at = document.get("available_at")
    created_at = document.get("created_at")
    return RunCommand(
        command_id=CommandId.parse(str(document["command_id"])),
        identity=identity,
        request=request,
        priority=QueuePriority(int(document["priority"])),
        available_at=(
            datetime.fromisoformat(str(available_at))
            if available_at is not None
            else datetime.now(UTC)
        ),
        created_at=(
            datetime.fromisoformat(str(created_at))
            if created_at is not None
            else datetime.now(UTC)
        ),
    )
