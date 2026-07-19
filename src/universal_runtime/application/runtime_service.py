from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime

from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.domain.events import RuntimeEvent
from universal_runtime.domain.execution import ExecutionRequest
from universal_runtime.domain.identity import EventId, ExecutionIdentity, ThreadId
from universal_runtime.domain.resources import (
    RunRecord,
    ThreadRecord,
    generated_thread_id,
)
from universal_runtime.ports.events import EventStore
from universal_runtime.ports.queue import RunCommandQueue
from universal_runtime.ports.repositories import RunRepository, ThreadRepository


class RuntimeExecutionService:
    def __init__(
        self,
        *,
        threads: ThreadRepository,
        runs: RunRepository,
        commands: RunCommandQueue,
        events: EventStore,
    ) -> None:
        self._threads = threads
        self._runs = runs
        self._commands = commands
        self._events = events
        self._identities: dict[str, ExecutionIdentity] = {}

    async def create_thread(
        self, thread_id: str | None = None, metadata: dict[str, object] | None = None
    ) -> ThreadRecord:
        thread = ThreadRecord(
            thread_id=ThreadId(thread_id) if thread_id else generated_thread_id(),
            metadata=metadata or {},
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
        )
        return await self._threads.create(thread)

    async def start_run(self, request: ExecutionRequest) -> RunRecord:
        thread_id = request.identity.thread_id
        if thread_id is not None:
            thread = await self._threads.get(str(thread_id))
            if thread.status == "busy":
                raise RuntimeFailure(ErrorCode.THREAD_BUSY, f"thread is busy: {thread_id}")
            await self._threads.update(replace(thread, status="busy", updated_at=datetime.now(UTC)))
        run = RunRecord(
            run_id=request.identity.run_id,
            thread_id=thread_id,
            assistant_id=request.identity.assistant_id,
            metadata=dict(request.metadata),
        )
        created = await self._runs.create(run)
        self._identities[str(run.run_id)] = request.identity
        try:
            await self._commands.publish(request)
        except Exception:
            await self._runs.update(replace(created, status="error"))
            if thread_id is not None:
                thread = await self._threads.get(str(thread_id))
                await self._threads.update(
                    replace(thread, status="error", updated_at=datetime.now(UTC))
                )
            raise
        await self._events.publish(self._event(request, "run.queued", 0))
        return created

    async def cancel_run(self, run_id: str) -> RunRecord:
        run = await self._runs.get(run_id)
        if run.status == "cancelled":
            return run
        cancelled = replace(run, status="cancelled")
        await self._runs.update(cancelled)
        if run.thread_id is not None:
            thread = await self._threads.get(str(run.thread_id))
            await self._threads.update(replace(thread, status="idle", updated_at=datetime.now(UTC)))
        await self._events.publish(self._event_from_run(run, "run.cancelled", 1))
        return cancelled

    async def stream_events(
        self, run_id: str, after_sequence: int = -1
    ) -> AsyncIterator[RuntimeEvent]:
        for event in await self._events.replay(run_id, after_sequence=after_sequence):
            yield event

    @staticmethod
    def _event(request: ExecutionRequest, event_type: str, sequence: int) -> RuntimeEvent:
        return RuntimeEvent(
            event_id=EventId.new(),
            sequence=sequence,
            timestamp=datetime.now(UTC),
            identity=request.identity,
            type=event_type,
            data={"assistant_id": request.assistant_id},
        )

    def _event_from_run(self, run: RunRecord, event_type: str, sequence: int) -> RuntimeEvent:
        identity = self._identities[str(run.run_id)]
        return RuntimeEvent(
            event_id=EventId.new(),
            sequence=sequence,
            timestamp=datetime.now(UTC),
            identity=identity,
            type=event_type,
            data={"run_id": str(run.run_id), "status": run.status},
        )
