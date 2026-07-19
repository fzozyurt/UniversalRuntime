from __future__ import annotations

import asyncio

import pytest

from universal_runtime.adapters.memory.events import InMemoryEventJournal
from universal_runtime.domain.events import RuntimeEventDraft, RuntimeEventType
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


def identity() -> ExecutionIdentity:
    return ExecutionIdentity(
        ApplicationScope(
            WorkspaceId.parse("w"),
            ProjectId.parse("p"),
            ApplicationId.parse("a"),
            RevisionId.parse("r"),
            DeploymentId.parse("d"),
        ),
        AssistantId.parse("assistant"),
        RunId.parse("run"),
        AttemptId.parse("attempt"),
        ThreadId.parse("thread"),
    )


@pytest.mark.asyncio
async def test_journal_replay_live_terminal_and_cursor() -> None:
    journal = InMemoryEventJournal()
    ident = identity()
    await journal.append(RuntimeEventDraft(ident, RuntimeEventType.RUN_QUEUED))
    await journal.append(RuntimeEventDraft(ident, RuntimeEventType.RUN_STARTED))

    async def collect() -> list[object]:
        return [event async for event in journal.subscribe(ident.run_id)]

    task = asyncio.create_task(collect())
    await asyncio.sleep(0)
    await journal.append(RuntimeEventDraft(ident, RuntimeEventType.RUN_COMPLETED))
    events = await asyncio.wait_for(task, timeout=1)
    assert [event.sequence for event in events] == [0, 1, 2]
    assert [event.type for event in await journal.replay(ident.run_id, after_sequence=1)] == [
        RuntimeEventType.RUN_COMPLETED
    ]


@pytest.mark.asyncio
async def test_journal_subscriber_cleanup_on_cancel() -> None:
    journal = InMemoryEventJournal()
    ident = identity()
    subscription = journal.subscribe(ident.run_id)
    task = asyncio.create_task(subscription.__anext__())
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await subscription.aclose()
    assert not journal._subscribers
