import asyncio
from datetime import UTC, datetime

import pytest

from universal_runtime.adapters.memory.capacity import ExecutionCapacity
from universal_runtime.adapters.memory.configuration import InMemoryApplicationConfigRepository
from universal_runtime.adapters.memory.queue import InMemoryPriorityQueue
from universal_runtime.adapters.memory.repositories import (
    InMemoryEventRepository,
    InMemoryRunRepository,
)
from universal_runtime.bootstrap.local import create_local_runtime
from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.domain.events import RuntimeEvent
from universal_runtime.domain.execution import ExecutionRequest, QueuePriority
from universal_runtime.domain.identity import (
    ApplicationId,
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
from universal_runtime.domain.resources import RunRecord


def make_identity(thread_id: str = "thread", run_id: str = "run") -> ExecutionIdentity:
    return ExecutionIdentity(
        workspace_id=WorkspaceId("workspace"),
        project_id=ProjectId("project"),
        application_id=ApplicationId("application"),
        revision_id=RevisionId("revision"),
        deployment_id=DeploymentId("deployment"),
        assistant_id=AssistantId("assistant"),
        thread_id=ThreadId(thread_id),
        run_id=RunId(run_id),
        attempt_id=AttemptId("attempt"),
    )


@pytest.mark.asyncio
async def test_priority_queue_is_interactive_first_and_reject_can_retry() -> None:
    queue = InMemoryPriorityQueue()
    batch = ExecutionRequest(
        make_identity(run_id="batch"), "assistant", priority=QueuePriority.BATCH
    )
    interactive = ExecutionRequest(
        make_identity(run_id="interactive"), "assistant", priority=QueuePriority.INTERACTIVE
    )
    await queue.publish(batch)
    await queue.publish(interactive)
    first = await queue.receive()
    assert first.identity.run_id == "interactive"
    await queue.reject(first, retryable=True)
    second = await queue.receive()
    assert second.identity.run_id == "interactive"
    await queue.acknowledge(second)
    third = await queue.receive()
    assert third.identity.run_id == "batch"
    await queue.acknowledge(third)


@pytest.mark.asyncio
async def test_in_memory_runs_enforce_one_active_run_per_thread() -> None:
    repository = InMemoryRunRepository()
    first = RunRecord(RunId("run-1"), ThreadId("thread"), AssistantId("assistant"))
    await repository.create(first)
    with pytest.raises(RuntimeFailure) as error:
        await repository.create(
            RunRecord(RunId("run-2"), ThreadId("thread"), AssistantId("assistant"))
        )
    assert error.value.code is ErrorCode.THREAD_BUSY
    await repository.update(
        RunRecord(RunId("run-1"), ThreadId("thread"), AssistantId("assistant"), "success")
    )
    await repository.create(RunRecord(RunId("run-2"), ThreadId("thread"), AssistantId("assistant")))


@pytest.mark.asyncio
async def test_config_revisions_are_hashable_immutable_and_activatable() -> None:
    repository = InMemoryApplicationConfigRepository()
    first = await repository.create_revision("application", {"b": 2, "a": 1})
    second = await repository.create_revision("application", {"a": 1, "b": 3})
    assert first.config_hash != second.config_hash
    assert (await repository.get_active("application")).revision == 1
    assert (await repository.activate("application", 2)).active is True
    assert (await repository.get_active("application")).revision == 2
    with pytest.raises(RuntimeFailure, match="active config"):
        await repository.get_active("missing")


@pytest.mark.asyncio
async def test_event_repository_replay_requires_monotonic_sequences() -> None:
    repository = InMemoryEventRepository()
    identity = make_identity()
    event = RuntimeEvent("event-1", 0, datetime.now(UTC), identity, "run.queued")
    await repository.append(event)
    assert await repository.replay("run", -1) == [event]
    with pytest.raises(RuntimeFailure, match="sequence"):
        await repository.append(
            RuntimeEvent("event-0", 0, datetime.now(UTC), identity, "run.started")
        )


@pytest.mark.asyncio
async def test_capacity_bounds_execution_and_drains_deterministically() -> None:
    capacity = ExecutionCapacity(1)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def active_run() -> None:
        async with capacity.slot():
            entered.set()
            await release.wait()

    task = asyncio.create_task(active_run())
    await entered.wait()
    draining = asyncio.create_task(capacity.drain())
    await asyncio.sleep(0)
    assert not draining.done()
    release.set()
    await task
    await draining
    assert capacity.active == 0
    with pytest.raises(RuntimeFailure, match="draining"):
        async with capacity.slot():
            pass


@pytest.mark.asyncio
async def test_local_composition_runs_queue_and_cancellation_flow() -> None:
    runtime = create_local_runtime(max_concurrency=2)
    thread = await runtime.execution.create_thread("thread")
    request = ExecutionRequest(make_identity(str(thread.thread_id)), "assistant")
    run = await runtime.execution.start_run(request)
    assert run.run_id == "run"
    assert (await runtime.commands.receive()).identity.run_id == "run"
    await runtime.commands.acknowledge(request)
    cancelled = await runtime.execution.cancel_run("run")
    assert cancelled.status == "cancelled"
    events = [event async for event in runtime.execution.stream_events("run")]
    assert [event.type for event in events] == ["run.queued", "run.cancelled"]
    await runtime.shutdown()
