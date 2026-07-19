from __future__ import annotations

from dataclasses import dataclass
from typing import Any

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
from universal_runtime.application.runtime_service import RuntimeExecutionService
from universal_runtime.ports.registry import AdapterRegistry


@dataclass(slots=True)
class LocalRuntime:
    config: InMemoryApplicationConfigRepository
    assistants: InMemoryAssistantRepository
    outbox: InMemoryOutboxRepository
    threads: InMemoryThreadRepository
    runs: InMemoryRunRepository
    events: InMemoryEventJournal
    commands: InMemoryPriorityQueue
    adapters: AdapterRegistry
    capacity: ExecutionCapacity
    execution: RuntimeExecutionService
    execute_locally: bool = True
    database_engine: Any | None = None

    async def start(self) -> None:
        if self.execute_locally:
            await self.execution.start_worker()

    async def shutdown(self) -> None:
        await self.execution.stop_worker()
        close = getattr(self.commands, "close", None)
        if close is not None:
            await close()
        await self.capacity.drain()
        if self.database_engine is not None:
            await self.database_engine.dispose()


def create_local_runtime(*, max_concurrency: int = 8) -> LocalRuntime:
    config = InMemoryApplicationConfigRepository()
    assistants = InMemoryAssistantRepository()
    outbox = InMemoryOutboxRepository()
    threads = InMemoryThreadRepository()
    runs = InMemoryRunRepository()
    events = InMemoryEventJournal()
    commands = InMemoryPriorityQueue()
    capacity = ExecutionCapacity(max_concurrency)
    adapters = InMemoryAdapterRegistry()
    return LocalRuntime(
        config=config,
        assistants=assistants,
        outbox=outbox,
        threads=threads,
        runs=runs,
        events=events,
        commands=commands,
        adapters=adapters,
        capacity=capacity,
        execution=RuntimeExecutionService(
            threads=threads,
            runs=runs,
            commands=commands,
            journal=events,
            replay=events,
            subscription=events,
            adapters=adapters,
            capacity=capacity,
        ),
    )
