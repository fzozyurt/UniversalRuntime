from __future__ import annotations

from typing import Protocol

from universal_runtime.domain.execution import ExecutionRequest
from universal_runtime.domain.resources import AssistantRecord, RunRecord, ThreadRecord


class AssistantRepository(Protocol):
    async def create(self, assistant: AssistantRecord) -> AssistantRecord: ...

    async def get(self, assistant_id: str) -> AssistantRecord: ...


class ThreadRepository(Protocol):
    async def create(self, thread: ThreadRecord) -> ThreadRecord: ...

    async def get(self, thread_id: str) -> ThreadRecord: ...

    async def update(self, thread: ThreadRecord) -> ThreadRecord: ...


class RunRepository(Protocol):
    async def create(self, run: RunRecord) -> RunRecord: ...

    async def get(self, run_id: str) -> RunRecord: ...

    async def update(self, run: RunRecord) -> RunRecord: ...

    async def active_for_thread(self, thread_id: str) -> RunRecord | None: ...


class ExecutionEventRepository(Protocol):
    async def append(self, event: object) -> None: ...

    async def replay(self, run_id: str, after_sequence: int = -1) -> list[object]: ...


class RequestValidator(Protocol):
    def validate(self, request: ExecutionRequest) -> None: ...
