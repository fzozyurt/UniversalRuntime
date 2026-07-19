from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from universal_runtime.domain.events import RuntimeEventDraft, RuntimeEventType
from universal_runtime.domain.execution import ExecutionRequest


def _event_type(mode: str, payload: Any) -> RuntimeEventType | str:
    if mode == "values":
        return RuntimeEventType.STATE_VALUES
    if mode == "updates":
        return RuntimeEventType.STATE_UPDATES
    if mode in {"messages", "messages-tuple"}:
        return RuntimeEventType.MESSAGE_DELTA
    if mode == "custom":
        return RuntimeEventType.CUSTOM
    if mode == "checkpoints":
        return RuntimeEventType.CHECKPOINT_CREATED
    if mode == "tasks":
        event_name = str(payload.get("event", "")) if isinstance(payload, dict) else ""
        return (
            RuntimeEventType.TASK_COMPLETED
            if event_name.endswith("end")
            else RuntimeEventType.TASK_STARTED
        )
    if mode in {"events", "debug"}:
        event_name = str(payload.get("event", "")) if isinstance(payload, dict) else ""
        if "tool" in event_name and event_name.endswith("start"):
            return RuntimeEventType.TOOL_STARTED
        if "tool" in event_name and event_name.endswith("end"):
            return RuntimeEventType.TOOL_COMPLETED
        if "chain" in event_name and event_name.endswith("start"):
            return RuntimeEventType.AGENT_STARTED
        if "chain" in event_name and event_name.endswith("end"):
            return RuntimeEventType.AGENT_COMPLETED
        return RuntimeEventType.CUSTOM
    return RuntimeEventType.CUSTOM


def _unpack(chunk: Any, default_mode: str) -> tuple[str, tuple[str, ...], Any]:
    if isinstance(chunk, dict) and {"type", "data"}.issubset(chunk):
        return str(chunk["type"]), tuple(str(item) for item in chunk.get("ns", ())), chunk["data"]
    if isinstance(chunk, tuple) and len(chunk) == 2:
        first, payload = chunk
        if isinstance(first, (tuple, list)) and all(isinstance(item, str) for item in first):
            return default_mode, tuple(first), payload
        if isinstance(first, str):
            return first, (), payload
    return default_mode, (), chunk


async def map_stream(
    chunks: AsyncIterator[Any], request: ExecutionRequest
) -> AsyncIterator[RuntimeEventDraft]:
    default_mode = request.stream_modes[0]
    async for chunk in chunks:
        mode, namespace, payload = _unpack(chunk, default_mode)
        if isinstance(payload, dict) and "ns" in payload and "data" in payload:
            namespace = tuple(str(item) for item in payload["ns"])
            payload = payload["data"]
        yield RuntimeEventDraft(
            request.identity,
            _event_type(mode, payload),
            namespace,
            payload,
            {"langgraph_mode": mode, "native_type": mode},
        )
