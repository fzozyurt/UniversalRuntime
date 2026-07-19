from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Protocol

from universal_runtime.domain.events import RuntimeEvent, RuntimeEventDraft
from universal_runtime.domain.identity import RunId


class EventJournal(Protocol):
    async def append(self, draft: RuntimeEventDraft) -> RuntimeEvent: ...


class EventReplay(Protocol):
    async def replay(
        self, run_id: RunId, *, after_sequence: int = -1
    ) -> tuple[RuntimeEvent, ...]: ...


class EventSubscription(Protocol):
    def subscribe(
        self, run_id: RunId, *, after_sequence: int = -1
    ) -> AsyncIterator[RuntimeEvent]: ...


class IntegrationEventPublisher(Protocol):
    async def publish(self, event: RuntimeEvent) -> None: ...
