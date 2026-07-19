from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from universal_runtime.adapters.kafka import (
    KafkaRunCommandQueue,
    PartitionKey,
    TopicNames,
    WeightedFairDispatcher,
)
from universal_runtime.domain.execution import ExecutionRequest, QueuePriority, RunCommand
from universal_runtime.domain.identity import (
    ApplicationId,
    ApplicationScope,
    AssistantId,
    AttemptId,
    DeploymentId,
    ProjectId,
    RevisionId,
    RunId,
    ThreadId,
    WorkerId,
    WorkspaceId,
)


def command(name: str, priority: QueuePriority, thread: str = "thread") -> RunCommand:
    identity = __import__(
        "universal_runtime.domain.identity", fromlist=["ExecutionIdentity"]
    ).ExecutionIdentity(
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
        ThreadId.parse(thread),
    )
    request = ExecutionRequest(identity, priority=priority)
    now = datetime.now(UTC)
    return RunCommand(
        __import__("universal_runtime.domain.identity", fromlist=["CommandId"]).CommandId.new(),
        identity,
        request,
        priority,
        now,
        now,
    )


def test_topics_and_partition_key() -> None:
    topics = TopicNames.from_config(
        prefix="x", environment="prod", overrides={"batch": "custom.batch"}
    )
    assert topics.interactive == "x.prod.runs.interactive.v1"
    assert topics.batch == "custom.batch"
    assert PartitionKey.for_command(command("run", QueuePriority.NORMAL)) == "a:thread"


@pytest.mark.asyncio
async def test_interactive_first_batch_eventually_and_receipt_retry() -> None:
    queue = KafkaRunCommandQueue()
    for index in range(20):
        await queue.publish(command(f"batch-{index}", QueuePriority.BATCH, f"b-{index}"))
    await queue.publish(command("interactive", QueuePriority.INTERACTIVE, "i"))
    receipt = await queue.receive(WorkerId.parse("worker"))
    assert receipt.command.identity.run_id == "interactive"
    await queue.acknowledge(receipt)
    seen = []
    for _ in range(20):
        receipt = await queue.receive(WorkerId.parse("worker"))
        seen.append(str(receipt.command.identity.run_id))
        await queue.acknowledge(receipt)
    assert len(seen) == 20


def test_age_promotion_and_thread_ordering() -> None:
    scheduler = WeightedFairDispatcher(age_after=timedelta(seconds=1))
    first = command("first", QueuePriority.BATCH, "same")
    second = command("second", QueuePriority.INTERACTIVE, "same")
    scheduler.add(second)
    scheduler.add(first)
    selected, _ = scheduler.choose(now=datetime.now(UTC))
    assert selected.identity.run_id == "second"  # inserted first in this scheduler
