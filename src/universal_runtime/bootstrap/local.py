from __future__ import annotations

from dataclasses import dataclass

from universal_runtime.adapters.memory.capacity import ExecutionCapacity
from universal_runtime.adapters.memory.configuration import InMemoryApplicationConfigRepository
from universal_runtime.adapters.memory.events import InMemoryEventJournal
from universal_runtime.adapters.memory.queue import InMemoryPriorityQueue
from universal_runtime.adapters.memory.registry import InMemoryAdapterRegistry
from universal_runtime.adapters.memory.repositories import (
    InMemoryRunRepository,
    InMemoryThreadRepository,
)
from universal_runtime.application.runtime_service import RuntimeExecutionService


@dataclass(slots=True)
class LocalRuntime:
    config: InMemoryApplicationConfigRepository
    threads: InMemoryThreadRepository
    runs: InMemoryRunRepository
    events: InMemoryEventJournal
    commands: InMemoryPriorityQueue
    adapters: InMemoryAdapterRegistry
    capacity: ExecutionCapacity
    execution: RuntimeExecutionService

    async def shutdown(self) -> None:
        await self.commands.close()
        await self.capacity.drain()


def create_local_runtime(*, max_concurrency: int = 8) -> LocalRuntime:
    config = InMemoryApplicationConfigRepository()
    threads = InMemoryThreadRepository()
    runs = InMemoryRunRepository()
    events = InMemoryEventJournal()
    commands = InMemoryPriorityQueue()
    capacity = ExecutionCapacity(max_concurrency)
    return LocalRuntime(
        config=config,
        threads=threads,
        runs=runs,
        events=events,
        commands=commands,
        adapters=InMemoryAdapterRegistry(),
        capacity=capacity,
        execution=RuntimeExecutionService(
            threads=threads,
            runs=runs,
            commands=commands,
            journal=events,
            replay=events,
            subscription=events,
        ),
    )
