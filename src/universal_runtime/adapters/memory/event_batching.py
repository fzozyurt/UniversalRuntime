from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from universal_runtime.domain.events import RuntimeEvent, RuntimeEventType


@dataclass(frozen=True, slots=True)
class EventBatchConfig:
    max_events: int = 100
    max_bytes: int = 262144
    flush_interval: float = 0.25


class RuntimeEventBatcher:
    def __init__(
        self,
        writer: Callable[[tuple[RuntimeEvent, ...]], Awaitable[None]],
        config: EventBatchConfig = EventBatchConfig(),
    ) -> None:
        if config.max_events < 1 or config.max_bytes < 1:
            raise ValueError("batch limits must be positive")
        self._writer = writer
        self._config = config
        self._events: list[RuntimeEvent] = []
        self._bytes = 0
        self._lock = asyncio.Lock()

    async def add(self, event: RuntimeEvent) -> None:
        size = len(str(event.data).encode("utf-8")) + len(str(event.native).encode("utf-8"))
        async with self._lock:
            if self._events and (
                len(self._events) >= self._config.max_events
                or self._bytes + size > self._config.max_bytes
            ):
                await self._flush_locked()
            self._events.append(event)
            self._bytes += size
            if event.type in {
                RuntimeEventType.RUN_COMPLETED,
                RuntimeEventType.RUN_CANCELLED,
                RuntimeEventType.RUN_FAILED,
                RuntimeEventType.RUN_TIMEOUT,
            }:
                await self._flush_locked()

    async def flush(self) -> None:
        async with self._lock:
            await self._flush_locked()

    async def _flush_locked(self) -> None:
        if not self._events:
            return
        events = tuple(self._events)
        self._events.clear()
        self._bytes = 0
        await self._writer(events)
