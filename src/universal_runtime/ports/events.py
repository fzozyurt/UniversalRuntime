from __future__ import annotations

from typing import Protocol

from universal_runtime.domain.events import RuntimeEvent


class EventPublisher(Protocol):
    async def publish(self, event: RuntimeEvent) -> None: ...


class EventReplay(Protocol):
    async def replay(self, run_id: str, after_sequence: int = -1) -> list[RuntimeEvent]: ...


class EventStore(EventPublisher, EventReplay, Protocol):
    """Combined port used by local execution services."""
