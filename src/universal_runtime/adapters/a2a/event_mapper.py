from __future__ import annotations

from typing import Any

from universal_runtime.domain.events import RuntimeEvent
from universal_runtime.domain.events.types import RuntimeEventType


def text_message(event: RuntimeEvent) -> Any | None:
    if event.type not in {RuntimeEventType.MESSAGE_DELTA, RuntimeEventType.MESSAGE_COMPLETED}:
        return None
    from a2a.types import Message, Part, Role

    value = event.data if isinstance(event.data, str) else str(event.data)
    return Message(
        message_id=str(event.event_id),
        context_id=str(event.identity.thread_id or ""),
        task_id=str(event.identity.run_id),
        role=Role.ROLE_AGENT,
        parts=[Part(text=value)],
    )
