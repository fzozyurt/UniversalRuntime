from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.domain.execution import ExecutionRequest


@dataclass(order=True, slots=True)
class _QueuedRequest:
    priority: int
    sequence: int
    request: ExecutionRequest = field(compare=False)


class InMemoryPriorityQueue:
    def __init__(self) -> None:
        self._queue: asyncio.PriorityQueue[_QueuedRequest] = asyncio.PriorityQueue()
        self._sequence = 0
        self._closed = False
        self._lock = asyncio.Lock()

    async def publish(self, request: ExecutionRequest) -> None:
        async with self._lock:
            if self._closed:
                raise RuntimeFailure(ErrorCode.QUEUE_CLOSED, "run command queue is closed")
            item = _QueuedRequest(-int(request.priority), self._sequence, request)
            self._sequence += 1
            await self._queue.put(item)

    async def receive(self) -> ExecutionRequest:
        if self._closed and self._queue.empty():
            raise RuntimeFailure(ErrorCode.QUEUE_CLOSED, "run command queue is closed")
        item = await self._queue.get()
        return item.request

    async def acknowledge(self, request: ExecutionRequest) -> None:
        del request
        self._queue.task_done()

    async def reject(self, request: ExecutionRequest, *, retryable: bool) -> None:
        self._queue.task_done()
        if retryable:
            await self.publish(request)

    async def close(self) -> None:
        async with self._lock:
            self._closed = True

    @property
    def pending(self) -> int:
        return self._queue.qsize()
