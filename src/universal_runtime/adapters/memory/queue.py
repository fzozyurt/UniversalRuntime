from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.domain.execution import ExecutionRequest, RunCommand, RunCommandReceipt
from universal_runtime.domain.identity import CommandId, LeaseId, WorkerId


@dataclass(order=True, slots=True)
class _QueuedCommand:
    priority: int
    sequence: int
    command: RunCommand = field(compare=False)
    delivery_count: int = field(compare=False, default=0)


class InMemoryPriorityQueue:
    def __init__(self, *, lease_seconds: int = 60, max_retries: int = 3) -> None:
        self._queue: asyncio.PriorityQueue[_QueuedCommand] = asyncio.PriorityQueue()
        self._sequence = 0
        self._closed = False
        self._lock = asyncio.Lock()
        self._leases: dict[str, tuple[_QueuedCommand, RunCommandReceipt]] = {}
        self._lease_seconds = lease_seconds
        self._max_retries = max_retries
        self.dead_letters: list[RunCommand] = []

    async def publish(self, command: RunCommand | ExecutionRequest) -> None:
        if isinstance(command, ExecutionRequest):
            now = datetime.now(UTC)
            command = RunCommand(
                CommandId.new(), command.identity, command, command.priority, now, now
            )
        async with self._lock:
            if self._closed:
                raise RuntimeFailure(ErrorCode.QUEUE_CLOSED, "run command queue is closed")
            await self._queue.put(_QueuedCommand(-int(command.priority), self._sequence, command))
            self._sequence += 1

    async def receive(self, worker_id: WorkerId | None = None) -> RunCommandReceipt:
        while True:
            if self._closed and self._queue.empty():
                raise RuntimeFailure(ErrorCode.QUEUE_CLOSED, "run command queue is closed")
            item = await self._queue.get()
            now = datetime.now(UTC)
            receipt = RunCommandReceipt(
                command=item.command,
                lease_id=LeaseId.new(),
                delivery_count=item.delivery_count + 1,
                leased_at=now,
                lease_expires_at=now + timedelta(seconds=self._lease_seconds),
            )
            async with self._lock:
                self._leases[str(receipt.lease_id)] = (item, receipt)
            del worker_id
            return receipt

    async def acknowledge(self, receipt: RunCommandReceipt) -> None:
        async with self._lock:
            if self._leases.pop(str(receipt.lease_id), None) is None:
                raise RuntimeFailure(ErrorCode.INVALID_EXECUTION_INPUT, "receipt is not active")
        self._queue.task_done()

    async def reject(self, receipt: RunCommandReceipt, *, retryable: bool) -> None:
        async with self._lock:
            entry = self._leases.pop(str(receipt.lease_id), None)
        if entry is None:
            raise RuntimeFailure(ErrorCode.INVALID_EXECUTION_INPUT, "receipt is not active")
        item, _ = entry
        self._queue.task_done()
        if retryable and receipt.delivery_count <= self._max_retries:
            async with self._lock:
                await self._queue.put(
                    _QueuedCommand(
                        -int(item.command.priority),
                        self._sequence,
                        item.command,
                        receipt.delivery_count,
                    )
                )
                self._sequence += 1
        else:
            self.dead_letters.append(item.command)

    async def close(self) -> None:
        async with self._lock:
            self._closed = True

    @property
    def pending(self) -> int:
        return self._queue.qsize()
