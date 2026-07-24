from __future__ import annotations

from datetime import UTC, datetime

import pytest

from universal_runtime.adapters.kafka import KafkaRunCommandQueue, PartitionKey, TopicNames
from universal_runtime.domain.execution import ExecutionRequest, QueuePriority, RunCommand
from universal_runtime.domain.identity import (
    ApplicationId,
    ApplicationScope,
    AssistantId,
    AttemptId,
    CommandId,
    DeploymentId,
    ExecutionIdentity,
    ProjectId,
    RevisionId,
    RunId,
    ThreadId,
    WorkerId,
    WorkspaceId,
)


def command(name: str, priority: QueuePriority, thread: str | None = "thread") -> RunCommand:
    identity = ExecutionIdentity(
        ApplicationScope(
            WorkspaceId.parse("w"),
            ProjectId.parse("p"),
            ApplicationId.parse("a"),
            RevisionId.parse("r"),
            DeploymentId.parse("d"),
        ),
        AssistantId.parse("asst"),
        RunId.parse(name),
        AttemptId.parse("attempt"),
        ThreadId.parse(thread) if thread else None,
    )
    request = ExecutionRequest(identity, priority=priority)
    now = datetime.now(UTC)
    return RunCommand(CommandId.new(), identity, request, priority, now, now)


def test_topics_and_partition_key() -> None:
    topics = TopicNames.from_config(
        prefix="x",
        environment="prod",
        application_id="app",
        overrides={"long_queue": "custom.long_queue"},
    )
    assert topics.short_queue == "x.prod.app.runs.short_queue.v1"
    assert topics.long_queue == "custom.long_queue"
    assert PartitionKey.for_command(command("run", QueuePriority.INTERACTIVE)) == "a:thread"
    assert PartitionKey.for_command(command("run", QueuePriority.INTERACTIVE, None)) == "a:run"


@pytest.mark.asyncio
async def test_priority_and_receipt_retry_without_dispatcher() -> None:
    queue = KafkaRunCommandQueue()
    await queue.publish(command("batch", QueuePriority.BATCH, "batch-thread"))
    await queue.publish(command("interactive", QueuePriority.INTERACTIVE, "interactive-thread"))

    receipt = await queue.receive(WorkerId.parse("worker"))
    assert str(receipt.command.identity.run_id) == "interactive"
    await queue.acknowledge(receipt)

    receipt = await queue.receive(WorkerId.parse("worker"))
    assert str(receipt.command.identity.run_id) == "batch"
    await queue.reject(receipt, retryable=True)

    retry = await queue.receive(WorkerId.parse("worker"))
    assert retry.delivery_count == 2
    await queue.reject(retry, retryable=False)
    assert [str(item.identity.run_id) for item in queue.dead_letters] == ["batch"]
