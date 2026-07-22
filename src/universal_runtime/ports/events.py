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

    async def replay_by_thread(
        self, thread_id: str, *, after_sequence: int = -1, limit: int = 0
    ) -> tuple[RuntimeEvent, ...]:
        """Replay events for a thread across all runs."""


class EventSubscription(Protocol):
    def subscribe(
        self, run_id: RunId, *, after_sequence: int = -1
    ) -> AsyncIterator[RuntimeEvent]: ...


class IntegrationEventPublisher(Protocol):
    async def publish(self, event: RuntimeEvent) -> None: ...


class RuntimeEventPublisher(Protocol):
    async def publish(self, event: RuntimeEvent) -> None: ...


class LifecycleEventPublisher(Protocol):
    async def publish(self, event: RuntimeEvent) -> None: ...
