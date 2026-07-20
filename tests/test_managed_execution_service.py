from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

import pytest

from universal_runtime.adapters.memory.events import InMemoryEventJournal
from universal_runtime.adapters.memory.queue import InMemoryPriorityQueue
from universal_runtime.adapters.memory.repositories import (
    InMemoryRunRepository,
    InMemoryThreadRepository,
)
from universal_runtime.application.managed_execution_service import ManagedExecutionService
from universal_runtime.domain.events import RuntimeEventType
from universal_runtime.domain.execution import (
    ExecutionRequest,
    ExecutionTarget,
    QueuePriority,
    Run,
    RunCommand,
    RunStatus,
    Thread,
    ThreadStatus,
)
from universal_runtime.domain.identity import (
    ApplicationId,
    ApplicationScope,
    AssistantId,
    AttemptId,
    DeploymentId,
    ExecutionIdentity,
    ProjectId,
    RevisionId,
    RunId,
    ThreadId,
    WorkspaceId,
)


def _scope() -> ApplicationScope:
    return ApplicationScope(
        WorkspaceId.parse("workspace"),
        ProjectId.parse("project"),
        ApplicationId.parse("application"),
        RevisionId.parse("revision"),
        DeploymentId.parse("deployment"),
    )


def _identity(*, run_id: str = "run-1") -> ExecutionIdentity:
    return ExecutionIdentity(
        _scope(),
        AssistantId.parse("assistant-1"),
        RunId.parse(run_id),
        AttemptId.parse("attempt-1"),
        ThreadId.parse("thread-1"),
    )


@dataclass
class CapturingSubmission:
    runs: InMemoryRunRepository
    threads: InMemoryThreadRepository
    run: Run | None = None
    command: RunCommand | None = None
    thread: Thread | None = None

    async def submit(
        self,
        run: Run,
        command: RunCommand,
        *,
        thread: Thread | None,
    ) -> Run:
        self.run = run
        self.command = command
        self.thread = thread
        if thread is not None:
            await self.threads.update(thread)
        return await self.runs.create(run)


@dataclass
class CapturingCancellation:
    calls: list[Run]

    async def cancel(self, run: Run) -> bool:
        self.calls.append(run)
        return True


def _service(
    *,
    runs: InMemoryRunRepository,
    threads: InMemoryThreadRepository,
    journal: InMemoryEventJournal,
    submission: CapturingSubmission | None = None,
    cancellation: CapturingCancellation | None = None,
) -> ManagedExecutionService:
    return ManagedExecutionService(
        submission=submission,
        cancellation=cancellation,
        threads=threads,
        runs=runs,
        commands=InMemoryPriorityQueue(),
        journal=journal,
        replay=journal,
        subscription=journal,
        execution_scope=_scope(),
    )


@pytest.mark.asyncio
async def test_managed_service_submits_run_thread_and_command_as_one_intent() -> None:
    runs = InMemoryRunRepository()
    threads = InMemoryThreadRepository()
    journal = InMemoryEventJournal()
    now = datetime.now(UTC)
    await threads.create(
        Thread(
            ThreadId.parse("thread-1"),
            ThreadStatus.IDLE,
            {"channel": "chat"},
            now,
            now,
        )
    )
    submission = CapturingSubmission(runs, threads)
    service = _service(
        runs=runs,
        threads=threads,
        journal=journal,
        submission=submission,
    )
    request = ExecutionRequest(
        identity=_identity(),
        input={"messages": []},
        priority=QueuePriority.INTERACTIVE,
        target=ExecutionTarget("graph-not-assistant", 3),
    )

    created = await service.start_run(request)

    assert created.target == ExecutionTarget("graph-not-assistant", 3)
    assert submission.run == created
    assert submission.command is not None
    assert submission.command.request.target == created.target
    assert submission.command.identity.assistant_id == "assistant-1"
    assert submission.command.request.target.graph_id == "graph-not-assistant"
    assert submission.thread is not None
    assert submission.thread.status is ThreadStatus.BUSY
    assert (await threads.get("thread-1")).status is ThreadStatus.BUSY
    events = await journal.replay(created.run_id)
    assert [event.type for event in events] == [RuntimeEventType.RUN_QUEUED]
    assert events[0].data["outbox"] is True


@pytest.mark.asyncio
async def test_managed_cancellation_is_durable_before_remote_notification() -> None:
    runs = InMemoryRunRepository()
    threads = InMemoryThreadRepository()
    journal = InMemoryEventJournal()
    now = datetime.now(UTC)
    thread = Thread(
        ThreadId.parse("thread-1"),
        ThreadStatus.BUSY,
        {},
        now,
        now,
    )
    await threads.create(thread)
    run = Run(
        identity=_identity(),
        status=RunStatus.RUNNING,
        created_at=now,
        updated_at=now,
        target=ExecutionTarget("graph-1", 2),
    )
    await runs.create(run)
    cancellation = CapturingCancellation([])
    service = _service(
        runs=runs,
        threads=threads,
        journal=journal,
        cancellation=cancellation,
    )

    cancelled = await service.cancel_run("run-1")

    assert cancelled.status is RunStatus.CANCELLED
    assert (await runs.get("run-1")).status is RunStatus.CANCELLED
    assert cancellation.calls
    assert cancellation.calls[0].status is RunStatus.CANCELLED
    assert (await threads.get("thread-1")).status is ThreadStatus.IDLE
    events = await journal.replay(cancelled.run_id)
    assert events[-1].type is RuntimeEventType.RUN_CANCELLED
    assert events[-1].data["execution_owner_notified"] is True
