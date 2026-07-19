from __future__ import annotations

import asyncio

from universal_runtime.domain.assistants import Assistant
from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.domain.execution import Run, RunStatus, Thread
from universal_runtime.domain.identity import AssistantId, RunId, ThreadId


class InMemoryAssistantRepository:
    def __init__(self) -> None:
        self._items: dict[str, Assistant] = {}
        self._lock = asyncio.Lock()

    async def create(self, assistant: Assistant) -> Assistant:
        async with self._lock:
            if str(assistant.assistant_id) in self._items:
                raise RuntimeFailure(ErrorCode.INVALID_EXECUTION_INPUT, "assistant already exists")
            self._items[str(assistant.assistant_id)] = assistant
            return assistant

    async def all(self) -> tuple[Assistant, ...]:
        async with self._lock:
            return tuple(self._items.values())

    async def get(self, assistant_id: str | AssistantId) -> Assistant:
        try:
            return self._items[str(assistant_id)]
        except KeyError as exc:
            raise RuntimeFailure(
                ErrorCode.RESOURCE_NOT_FOUND, f"assistant not found: {assistant_id}"
            ) from exc


class InMemoryThreadRepository:
    def __init__(self) -> None:
        self._items: dict[str, Thread] = {}
        self._lock = asyncio.Lock()

    async def create(self, thread: Thread) -> Thread:
        async with self._lock:
            if str(thread.thread_id) in self._items:
                raise RuntimeFailure(ErrorCode.INVALID_EXECUTION_INPUT, "thread already exists")
            self._items[str(thread.thread_id)] = thread
            return thread

    async def get(self, thread_id: str | ThreadId) -> Thread:
        try:
            return self._items[str(thread_id)]
        except KeyError as exc:
            raise RuntimeFailure(
                ErrorCode.RESOURCE_NOT_FOUND, f"thread not found: {thread_id}"
            ) from exc

    async def update(self, thread: Thread) -> Thread:
        async with self._lock:
            if str(thread.thread_id) not in self._items:
                raise RuntimeFailure(
                    ErrorCode.RESOURCE_NOT_FOUND, f"thread not found: {thread.thread_id}"
                )
            self._items[str(thread.thread_id)] = thread
            return thread


class InMemoryRunRepository:
    def __init__(self) -> None:
        self._items: dict[str, Run] = {}
        self._lock = asyncio.Lock()

    async def create(self, run: Run) -> Run:
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

    async def get(self, run_id: str | RunId) -> Run:
        try:
            return self._items[str(run_id)]
        except KeyError as exc:
            raise RuntimeFailure(ErrorCode.RUN_NOT_FOUND, f"run not found: {run_id}") from exc

    async def update(self, run: Run) -> Run:
        async with self._lock:
            if str(run.run_id) not in self._items:
                raise RuntimeFailure(ErrorCode.RUN_NOT_FOUND, f"run not found: {run.run_id}")
            self._items[str(run.run_id)] = run
            return run

    async def active_for_thread(self, thread_id: str | ThreadId) -> Run | None:
        async with self._lock:
            return self._active_for_thread(str(thread_id))

    def _active_for_thread(self, thread_id: str) -> Run | None:
        active = {RunStatus.PENDING, RunStatus.RUNNING, RunStatus.INTERRUPTED}
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
