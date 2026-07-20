from __future__ import annotations

from dataclasses import dataclass

from universal_runtime.adapters.memory.capacity import ExecutionCapacity
from universal_runtime.adapters.memory.configuration import InMemoryApplicationConfigRepository
from universal_runtime.adapters.memory.events import InMemoryEventJournal
from universal_runtime.adapters.memory.outbox import InMemoryOutboxRepository
from universal_runtime.adapters.memory.queue import InMemoryPriorityQueue
from universal_runtime.adapters.memory.registry import InMemoryAdapterRegistry
from universal_runtime.adapters.memory.repositories import (
    InMemoryAssistantRepository,
    InMemoryRunRepository,
    InMemoryThreadRepository,
)
from universal_runtime.application.managed_execution_service import ManagedExecutionService
from universal_runtime.application.runtime_service import RuntimeExecutionService


@dataclass(slots=True)
class LocalRuntime:
    config: InMemoryApplicationConfigRepository
    assistants: InMemoryAssistantRepository
    outbox: InMemoryOutboxRepository | None
    threads: InMemoryThreadRepository
    runs: InMemoryRunRepository
    events: InMemoryEventJournal
    commands: InMemoryPriorityQueue
    adapters: InMemoryAdapterRegistry
    capacity: ExecutionCapacity
    execution: RuntimeExecutionService
    execute_locally: bool = True

    async def start(self) -> None:
        if self.execute_locally:
            await self.execution.start_worker()

    async def shutdown(self) -> None:
        await self.execution.stop_worker()
        close = getattr(self.commands, "close", None)
        if close is not None:
            await close()
        await self.capacity.drain()


def create_local_runtime(*, max_concurrency: int = 8) -> LocalRuntime:
    config = InMemoryApplicationConfigRepository()
    assistants = InMemoryAssistantRepository()
    threads = InMemoryThreadRepository()
    runs = InMemoryRunRepository()
    events = InMemoryEventJournal()
    commands = InMemoryPriorityQueue()
    capacity = ExecutionCapacity(max_concurrency)
    adapters = InMemoryAdapterRegistry()
    return LocalRuntime(
        config=config,
        assistants=assistants,
        outbox=None,
        threads=threads,
        runs=runs,
        events=events,
        commands=commands,
        adapters=adapters,
        capacity=capacity,
        execution=ManagedExecutionService(
            threads=threads,
            runs=runs,
            commands=commands,
            journal=events,
            replay=events,
            subscription=events,
            assistants=assistants,
            adapters=adapters,
            capacity=capacity,
        ),
    )
