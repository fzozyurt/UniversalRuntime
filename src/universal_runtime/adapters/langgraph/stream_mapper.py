from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from universal_runtime.domain.events import RuntimeEventDraft, RuntimeEventType
from universal_runtime.domain.execution import ExecutionRequest


async def map_stream(
    chunks: AsyncIterator[Any], request: ExecutionRequest
) -> AsyncIterator[RuntimeEventDraft]:
    async for chunk in chunks:
        mode, payload = (
            chunk
            if isinstance(chunk, tuple) and len(chunk) == 2
            else (request.stream_modes[0], chunk)
        )
        event_type = {
            "values": RuntimeEventType.STATE_VALUES,
            "updates": RuntimeEventType.STATE_UPDATES,
            "custom": RuntimeEventType.CUSTOM,
            "messages": RuntimeEventType.MESSAGE_DELTA,
            "messages-tuple": RuntimeEventType.MESSAGE_DELTA,
        }.get(str(mode), RuntimeEventType.CUSTOM)
        namespace: tuple[str, ...] = ()
        data = payload
        if isinstance(payload, dict) and "ns" in payload and "data" in payload:
            namespace = tuple(str(item) for item in payload["ns"])
            data = payload["data"]
        yield RuntimeEventDraft(
            request.identity, event_type, namespace, data, {"langgraph_mode": str(mode)}
        )
