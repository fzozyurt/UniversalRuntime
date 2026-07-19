from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime

from universal_runtime.domain.events import (
    RuntimeEvent,
    RuntimeEventDraft,
    RuntimeEventType,
    TraceContext,
)
from universal_runtime.domain.identity import EventId, RunId
from universal_runtime.ports.events import EventJournal, EventReplay, EventSubscription

_TERMINAL = {
    RuntimeEventType.RUN_COMPLETED,
    RuntimeEventType.RUN_CANCELLED,
    RuntimeEventType.RUN_FAILED,
    RuntimeEventType.RUN_TIMEOUT,
}


class InMemoryEventJournal(EventJournal, EventReplay, EventSubscription):
    def __init__(self, *, queue_size: int = 128) -> None:
        self._events: dict[str, list[RuntimeEvent]] = defaultdict(list)
        self._subscribers: dict[str, set[asyncio.Queue[RuntimeEvent | None]]] = defaultdict(set)
        self._lock = asyncio.Lock()
        self._queue_size = queue_size

    async def append(self, draft: RuntimeEventDraft) -> RuntimeEvent:
        async with self._lock:
            key = str(draft.identity.run_id)
            event = RuntimeEvent(
                EventId.new(),
                len(self._events[key]),
                datetime.now(UTC),
                draft.identity,
                draft.type,
                draft.namespace,
                draft.data,
                draft.native,
                TraceContext(),
            )
            self._events[key].append(event)
            for queue in tuple(self._subscribers[key]):
                try:
                    queue.put_nowait(event)
                except asyncio.QueueFull as exc:
                    raise RuntimeError("event subscriber backpressure") from exc
            if event.type in _TERMINAL:
                for queue in tuple(self._subscribers[key]):
                    queue.put_nowait(None)
            return event

    async def replay(self, run_id: RunId, *, after_sequence: int = -1) -> Sequence[RuntimeEvent]:
        async with self._lock:
            return tuple(
                e for e in self._events.get(str(run_id), ()) if e.sequence > after_sequence
            )

    async def subscribe(
        self, run_id: RunId, *, after_sequence: int = -1
    ) -> AsyncIterator[RuntimeEvent]:
        key = str(run_id)
        queue: asyncio.Queue[RuntimeEvent | None] = asyncio.Queue(self._queue_size)
        async with self._lock:
            initial = tuple(e for e in self._events.get(key, ()) if e.sequence > after_sequence)
            terminal = any(e.type in _TERMINAL for e in initial)
            if not terminal:
                self._subscribers[key].add(queue)
        try:
            for event in initial:
                yield event
            if terminal:
                return
            while True:
                event = await queue.get()
                if event is None:
                    return
                yield event
        finally:
            async with self._lock:
                self._subscribers[key].discard(queue)
                if not self._subscribers[key]:
                    self._subscribers.pop(key, None)
