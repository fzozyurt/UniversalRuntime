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


@dataclass(slots=True)
class LocalRuntime:
    """Runtime composition shared by local and production profiles.

    ``events`` is optional because framework-managed persistence (for example
    LangGraph checkpoint/store) is authoritative in production. Local mode keeps
    the in-memory journal solely to provide deterministic development replay.
    """

    config: Any
    assistants: Any
    outbox: Any | None
    threads: Any
    runs: Any
    events: Any | None
    commands: Any
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
