from __future__ import annotations

from typing import Protocol

from universal_runtime.domain.assistants import Assistant
from universal_runtime.domain.execution import ExecutionRequest, Run, Thread
from universal_runtime.domain.identity import ApplicationScope


class AssistantRepository(Protocol):
    async def create(self, assistant: Assistant) -> Assistant: ...
    async def get(self, assistant_id: str) -> Assistant: ...
    async def all(self) -> tuple[Assistant, ...]: ...
    async def update(
        self,
        assistant_id: str,
        assistant: Assistant,
    ) -> Assistant: ...
    async def delete(
        self,
        assistant_id: str,
        *,
        delete_threads: bool = False,
    ) -> None: ...
    async def versions(self, assistant_id: str) -> tuple[Assistant, ...]: ...
    async def set_latest(
        self,
        assistant_id: str,
        version: int,
    ) -> Assistant: ...
    async def count(self, *, graph_id: str | None = None) -> int: ...


class ThreadRepository(Protocol):
    async def create(self, thread: Thread) -> Thread: ...
    async def get(self, thread_id: str) -> Thread: ...
    async def update(self, thread: Thread) -> Thread: ...
    async def delete(self, thread_id: str) -> None: ...
    async def count(
        self,
        *,
        metadata: dict[str, object] | None = None,
        status: str | None = None,
    ) -> int: ...
    async def search(
        self,
        *,
        metadata: dict[str, object] | None = None,
        status: str | None = None,
        limit: int = 10,
        offset: int = 0,
    ) -> tuple[Thread, ...]: ...


class ThreadApplicationBinder(Protocol):
    async def bind(self, thread_id: str, scope: ApplicationScope) -> None: ...


class RunRepository(Protocol):
    async def create(self, run: Run) -> Run: ...
    async def get(self, run_id: str) -> Run: ...
    async def update(self, run: Run) -> Run: ...
    async def delete(self, run_id: str) -> None: ...
    async def active_for_thread(self, thread_id: str) -> Run | None: ...
    async def latest_for_thread(self, thread_id: str) -> Run | None: ...
    async def list_for_thread(
        self,
        thread_id: str,
        *,
        limit: int = 10,
        offset: int = 0,
        status: str | None = None,
    ) -> tuple[Run, ...]: ...


class RequestValidator(Protocol):
    def validate(self, request: ExecutionRequest) -> None: ...
