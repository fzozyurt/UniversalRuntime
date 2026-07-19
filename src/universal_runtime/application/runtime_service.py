from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime

from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.domain.events import RuntimeEvent, RuntimeEventDraft, RuntimeEventType
from universal_runtime.domain.execution import Run, RunError, Thread
from universal_runtime.domain.execution.requests import ExecutionRequest
from universal_runtime.domain.identity import RunId, ThreadId
from universal_runtime.domain.primitives.json_types import JsonObject
from universal_runtime.ports.events import EventJournal, EventReplay, EventSubscription
from universal_runtime.ports.queue import RunCommandQueue
from universal_runtime.ports.repositories import RunRepository, ThreadRepository


def _now() -> datetime:
    return datetime.now(UTC)


class RuntimeExecutionService:
    def __init__(
        self,
        *,
        threads: ThreadRepository,
        runs: RunRepository,
        commands: RunCommandQueue,
        journal: EventJournal,
        replay: EventReplay | None = None,
        subscription: EventSubscription | None = None,
    ) -> None:
        self._threads = threads
        self._runs = runs
        self._commands = commands
        self._journal = journal
        self._replay = replay
        self._subscription = subscription

    async def create_thread(
        self, thread_id: str | None = None, metadata: JsonObject | None = None
    ) -> Thread:
        now = _now()
        thread = Thread(
            thread_id=ThreadId.parse(thread_id) if thread_id else ThreadId.new(),
            metadata=metadata or {},
            created_at=now,
            updated_at=now,
        )
        return await self._threads.create(thread)

    async def start_run(self, request: ExecutionRequest) -> Run:
        now = _now()
        thread_id = request.identity.thread_id
        if thread_id is not None:
            thread = await self._threads.get(str(thread_id))
            if thread.status.value == "busy":
                raise RuntimeFailure(ErrorCode.THREAD_BUSY, f"thread is busy: {thread_id}")
            await self._threads.update(thread.mark_busy(now))
        run = Run(
            identity=request.identity,
            metadata=request.metadata,
            created_at=now,
            updated_at=now,
        )
        created = await self._runs.create(run)
        try:
            await self._commands.publish(request)
            await self._journal.append(
                RuntimeEventDraft(
                    request.identity,
                    RuntimeEventType.RUN_QUEUED,
                    data={"assistant_id": str(request.identity.assistant_id)},
                )
            )
        except Exception:
            await self._runs.update(
                created.fail(
                    RunError("RUN_QUEUE_FAILED", "run command could not be queued"), _now()
                )
            )
            if thread_id is not None:
                thread = await self._threads.get(str(thread_id))
                await self._threads.update(thread.mark_error(_now()))
            raise
        return created

    async def cancel_run(self, run_id: str) -> Run:
        run = await self._runs.get(run_id)
        cancelled = run.cancel(_now())
        if cancelled is not run:
            await self._runs.update(cancelled)
        if run.thread_id is not None:
            thread = await self._threads.get(str(run.thread_id))
            await self._threads.update(thread.mark_idle(_now()))
        await self._journal.append(
            RuntimeEventDraft(
                run.identity,
                RuntimeEventType.RUN_CANCELLED,
                data={"run_id": str(run.run_id), "status": str(cancelled.status)},
            )
        )
        return cancelled

    async def stream_events(
        self, run_id: str, after_sequence: int = -1
    ) -> AsyncIterator[RuntimeEvent]:
        if self._replay is None:
            raise RuntimeFailure(ErrorCode.ADAPTER_NOT_SUPPORTED, "event replay is not configured")
        for event in await self._replay.replay(RunId.parse(run_id), after_sequence=after_sequence):
            yield event

    async def stream_live_events(
        self, run_id: str, after_sequence: int = -1
    ) -> AsyncIterator[RuntimeEvent]:
        if self._subscription is None:
            raise RuntimeFailure(
                ErrorCode.ADAPTER_NOT_SUPPORTED, "event subscription is not configured"
            )
        async for event in self._subscription.subscribe(
            RunId.parse(run_id), after_sequence=after_sequence
        ):
            yield event
