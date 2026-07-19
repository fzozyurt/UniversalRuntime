from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from universal_runtime.domain.errors import ErrorCode, RuntimeFailure


class ExecutionCapacity:
    """A bounded async capacity with a deterministic draining state."""

    def __init__(self, maximum: int) -> None:
        if maximum < 1:
            raise ValueError("maximum capacity must be at least one")
        self.maximum = maximum
        self._semaphore = asyncio.Semaphore(maximum)
        self._active = 0
        self._draining = False
        self._condition = asyncio.Condition()

    @asynccontextmanager
    async def slot(self) -> AsyncIterator[None]:
        async with self._condition:
            if self._draining:
                raise RuntimeFailure(ErrorCode.QUEUE_CLOSED, "execution capacity is draining")
        await self._semaphore.acquire()
        async with self._condition:
            if self._draining:
                self._semaphore.release()
                raise RuntimeFailure(ErrorCode.QUEUE_CLOSED, "execution capacity is draining")
            self._active += 1
        try:
            yield
        finally:
            async with self._condition:
                self._active -= 1
                self._condition.notify_all()
            self._semaphore.release()

    async def start_draining(self) -> None:
        async with self._condition:
            self._draining = True
            self._condition.notify_all()

    async def drain(self) -> None:
        await self.start_draining()
        async with self._condition:
            await self._condition.wait_for(lambda: self._active == 0)

    @property
    def active(self) -> int:
        return self._active

    @property
    def draining(self) -> bool:
        return self._draining
