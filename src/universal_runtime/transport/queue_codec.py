from __future__ import annotations

from datetime import UTC, datetime

from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
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
from universal_runtime.domain.primitives.json_types import JsonObject, JsonValue


def _invalid(field: str, expected: str) -> RuntimeFailure:
    return RuntimeFailure(
        ErrorCode.INVALID_EXECUTION_INPUT,
        f"queue document field '{field}' must be {expected}",
    )


def _object(value: JsonValue, field: str) -> JsonObject:
    if not isinstance(value, dict):
        raise _invalid(field, "an object")
    return value


def _integer(
    value: JsonValue,
    field: str,
    *,
    default: int | None = None,
) -> int:
    if value is None and default is not None:
        return default
    if isinstance(value, bool) or not isinstance(value, int):
        raise _invalid(field, "an integer")
    return value


def _string(
    value: JsonValue,
    field: str,
    *,
    default: str | None = None,
) -> str:
    if value is None and default is not None:
        return default
    if not isinstance(value, str) or not value:
        raise _invalid(field, "a non-empty string")
    return value


def _optional_string(value: JsonValue, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise _invalid(field, "a string or null")
    return value


def _string_tuple(value: JsonValue, field: str) -> tuple[str, ...]:
    if value is None:
        return ("values",)
    if not isinstance(value, list):
        raise _invalid(field, "an array of strings")
    result: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise _invalid(field, "an array of strings")
        result.append(item)
    return tuple(result)


def _boolean(
    value: JsonValue,
    field: str,
    *,
    default: bool = False,
) -> bool:
    if value is None:
        return default
    if not isinstance(value, bool):
        raise _invalid(field, "a boolean")
    return value


def _priority(
    value: JsonValue,
    field: str,
    *,
    default: QueuePriority,
) -> QueuePriority:
    raw = _integer(value, field, default=int(default))
    try:
        return QueuePriority(raw)
    except ValueError as exc:
        raise _invalid(field, "a supported queue priority") from exc


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
    raw_identity = _object(document.get("identity"), "identity")
    identity = ExecutionIdentity(
        ApplicationScope(
            WorkspaceId.parse(
                _string(
                    raw_identity.get("workspace_id"),
                    "identity.workspace_id",
                )
            ),
            ProjectId.parse(
                _string(
                    raw_identity.get("project_id"),
                    "identity.project_id",
                )
            ),
            ApplicationId.parse(
                _string(
                    raw_identity.get("application_id"),
                    "identity.application_id",
                )
            ),
            RevisionId.parse(
                _string(
                    raw_identity.get("revision_id"),
                    "identity.revision_id",
                )
            ),
            DeploymentId.parse(
                _string(
                    raw_identity.get("deployment_id"),
                    "identity.deployment_id",
                )
            ),
        ),
        AssistantId.parse(
            _string(
                raw_identity.get("assistant_id"),
                "identity.assistant_id",
            )
        ),
        RunId.parse(
            _string(raw_identity.get("run_id"), "identity.run_id")
        ),
        AttemptId.parse(
            _string(
                raw_identity.get("attempt_id"),
                "identity.attempt_id",
            )
        ),
        (
            ThreadId.parse(thread_id)
            if (
                thread_id := _optional_string(
                    raw_identity.get("thread_id"),
                    "identity.thread_id",
                )
            )
            else None
        ),
    )
    raw_request = _object(document.get("request"), "request")
    raw_target = _object(raw_request.get("target") or {}, "request.target")
    request = ExecutionRequest(
        identity=identity,
        target=ExecutionTarget(
            _string(
                raw_target.get("graph_id"),
                "request.target.graph_id",
                default=str(identity.assistant_id),
            ),
            _integer(
                raw_target.get("assistant_version"),
                "request.target.assistant_version",
                default=1,
            ),
        ),
        input=raw_request.get("input"),
        command=raw_request.get("command"),
        config=_object(raw_request.get("config") or {}, "request.config"),
        context=_object(raw_request.get("context") or {}, "request.context"),
        metadata=_object(
            raw_request.get("metadata") or {},
            "request.metadata",
        ),
        stream_modes=_string_tuple(
            raw_request.get("stream_modes"),
            "request.stream_modes",
        ),
        stream_subgraphs=_boolean(
            raw_request.get("stream_subgraphs"),
            "request.stream_subgraphs",
        ),
        priority=_priority(
            raw_request.get("priority"),
            "request.priority",
            default=QueuePriority.INTERACTIVE,
        ),
        timeout_seconds=_integer(
            raw_request.get("timeout_seconds"),
            "request.timeout_seconds",
            default=1800,
        ),
        checkpoint_namespace=(
            _optional_string(
                raw_request.get("checkpoint_namespace"),
                "request.checkpoint_namespace",
            )
            or ""
        ),
        checkpoint_id=_optional_string(
            raw_request.get("checkpoint_id"),
            "request.checkpoint_id",
        ),
    )
    available_at = _optional_string(
        document.get("available_at"),
        "available_at",
    )
    created_at = _optional_string(
        document.get("created_at"),
        "created_at",
    )
    return RunCommand(
        command_id=CommandId.parse(
            _string(document.get("command_id"), "command_id")
        ),
        identity=identity,
        request=request,
        priority=_priority(
            document.get("priority"),
            "priority",
            default=request.priority,
        ),
        available_at=(
            datetime.fromisoformat(available_at)
            if available_at is not None
            else datetime.now(UTC)
        ),
        created_at=(
            datetime.fromisoformat(created_at)
            if created_at is not None
            else datetime.now(UTC)
        ),
    )
