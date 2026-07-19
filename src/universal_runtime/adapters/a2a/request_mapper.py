from __future__ import annotations

from typing import Any, cast

from universal_runtime.adapters.a2a.errors import invalid_context_id, unsupported_part
from universal_runtime.domain.execution import ExecutionRequest, QueuePriority
from universal_runtime.domain.identity import (
    ApplicationScope,
    AssistantId,
    AttemptId,
    ExecutionIdentity,
    RunId,
    ThreadId,
)
from universal_runtime.domain.primitives.json_types import JsonValue


def _part_value(part: Any) -> JsonValue:
    field = part.WhichOneof("content")
    if field == "text":
        return str(part.text)
    if field == "data":
        from google.protobuf.json_format import MessageToDict  # type: ignore[import-untyped]

        return cast(JsonValue, MessageToDict(part.data, preserving_proto_field_name=True))
    if field in {"raw", "url"}:
        raise unsupported_part(field)
    raise unsupported_part(field or "unknown")


def message_input(message: Any) -> JsonValue:
    values = [_part_value(part) for part in message.parts]
    return values[0] if len(values) == 1 else values


def context_thread_id(value: str | None) -> ThreadId | None:
    if value is None:
        return None
    if not value:
        raise invalid_context_id(value)
    try:
        return ThreadId.parse(value)
    except ValueError as exc:
        raise invalid_context_id(value) from exc


def execution_request(
    *,
    message: Any,
    assistant_id: AssistantId,
    run_id: RunId,
    scope: ApplicationScope,
) -> ExecutionRequest:
    thread_id = context_thread_id(str(message.context_id) if message.context_id else None)
    identity = ExecutionIdentity(
        scope=scope,
        assistant_id=assistant_id,
        run_id=run_id,
        attempt_id=AttemptId.new(),
        thread_id=thread_id,
    )
    return ExecutionRequest(
        identity=identity,
        input=message_input(message),
        metadata={"a2a.message_id": str(message.message_id)},
        stream_modes=("events",),
        priority=QueuePriority.INTERACTIVE,
    )
