from __future__ import annotations

import asyncio

from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.domain.events import RuntimeEvent
from universal_runtime.domain.resources import AssistantRecord, RunRecord, ThreadRecord


class InMemoryAssistantRepository:
    def __init__(self) -> None:
        self._items: dict[str, AssistantRecord] = {}
        self._lock = asyncio.Lock()

    async def create(self, assistant: AssistantRecord) -> AssistantRecord:
        async with self._lock:
            if str(assistant.assistant_id) in self._items:
                raise RuntimeFailure(ErrorCode.INVALID_EXECUTION_INPUT, "assistant already exists")
            self._items[str(assistant.assistant_id)] = assistant
            return assistant

    async def get(self, assistant_id: str) -> AssistantRecord:
        try:
            return self._items[assistant_id]
        except KeyError as exc:
            raise RuntimeFailure(
                ErrorCode.RESOURCE_NOT_FOUND, f"assistant not found: {assistant_id}"
            ) from exc


class InMemoryThreadRepository:
    def __init__(self) -> None:
        self._items: dict[str, ThreadRecord] = {}
        self._lock = asyncio.Lock()

    async def create(self, thread: ThreadRecord) -> ThreadRecord:
        async with self._lock:
            if str(thread.thread_id) in self._items:
                raise RuntimeFailure(ErrorCode.INVALID_EXECUTION_INPUT, "thread already exists")
            self._items[str(thread.thread_id)] = thread
            return thread

    async def get(self, thread_id: str) -> ThreadRecord:
        try:
            return self._items[thread_id]
        except KeyError as exc:
            raise RuntimeFailure(
                ErrorCode.RESOURCE_NOT_FOUND, f"thread not found: {thread_id}"
            ) from exc

    async def update(self, thread: ThreadRecord) -> ThreadRecord:
        async with self._lock:
            if str(thread.thread_id) not in self._items:
                raise RuntimeFailure(
                    ErrorCode.RESOURCE_NOT_FOUND, f"thread not found: {thread.thread_id}"
                )
            self._items[str(thread.thread_id)] = thread
            return thread


class InMemoryRunRepository:
    def __init__(self) -> None:
        self._items: dict[str, RunRecord] = {}
        self._lock = asyncio.Lock()

    async def create(self, run: RunRecord) -> RunRecord:
        async with self._lock:
            if str(run.run_id) in self._items:
                raise RuntimeFailure(ErrorCode.INVALID_EXECUTION_INPUT, "run already exists")
            if (
                run.thread_id is not None
                and self._active_for_thread(str(run.thread_id)) is not None
            ):
                raise RuntimeFailure(
                    ErrorCode.THREAD_BUSY, f"thread already has an active run: {run.thread_id}"
                )
            self._items[str(run.run_id)] = run
            return run

    async def get(self, run_id: str) -> RunRecord:
        try:
            return self._items[run_id]
        except KeyError as exc:
            raise RuntimeFailure(ErrorCode.RUN_NOT_FOUND, f"run not found: {run_id}") from exc

    async def update(self, run: RunRecord) -> RunRecord:
        async with self._lock:
            if str(run.run_id) not in self._items:
                raise RuntimeFailure(ErrorCode.RUN_NOT_FOUND, f"run not found: {run.run_id}")
            self._items[str(run.run_id)] = run
            return run

    async def active_for_thread(self, thread_id: str) -> RunRecord | None:
        async with self._lock:
            return self._active_for_thread(thread_id)

    def _active_for_thread(self, thread_id: str) -> RunRecord | None:
        active = {"pending", "running", "interrupted"}
        return next(
            (
                run
                for run in self._items.values()
                if run.thread_id is not None
                and str(run.thread_id) == thread_id
                and run.status in active
            ),
            None,
        )


class InMemoryEventRepository:
    def __init__(self) -> None:
        self._events: dict[str, list[RuntimeEvent]] = {}
        self._condition = asyncio.Condition()

    async def append(self, event: RuntimeEvent) -> None:
        async with self._condition:
            events = self._events.setdefault(str(event.identity.run_id), [])
            if events and event.sequence <= events[-1].sequence:
                raise RuntimeFailure(
                    ErrorCode.INVALID_EXECUTION_INPUT, "event sequence must increase"
                )
            events.append(event)
            self._condition.notify_all()

    async def publish(self, event: RuntimeEvent) -> None:
        await self.append(event)

    async def replay(self, run_id: str, after_sequence: int = -1) -> list[RuntimeEvent]:
        async with self._condition:
            return [
                event for event in self._events.get(run_id, []) if event.sequence > after_sequence
            ]
