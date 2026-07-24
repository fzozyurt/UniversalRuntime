from __future__ import annotations

import heapq
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.domain.execution import RunCommand, RunCommandReceipt
from universal_runtime.domain.identity import LeaseId, WorkerId
from universal_runtime.ports.queue import RunCommandQueue

from .partitioning import PartitionKey
from .topics import TopicNames


@dataclass(frozen=True, slots=True)
class KafkaMessage:
    topic: str
    key: str
    value: object
    headers: tuple[tuple[str, str], ...]


class InMemoryKafkaTransport:
    """Deterministic Kafka-shaped transport used by local and unit tests."""

    def __init__(
        self,
        topics: TopicNames | None = None,
        *,
        prefix: str = "rt",
        environment: str = "local",
    ) -> None:
        self.topics = topics or TopicNames.from_config(
            prefix=prefix,
            environment=environment,
            application_id="default",
        )
        self.prefix = prefix
        self.environment = environment
        self.messages: list[KafkaMessage] = []

    async def publish(self, command: RunCommand) -> None:
        topic = TopicNames.run_topic_for(
            self.prefix,
            str(command.identity.application_id),
            int(command.priority),
            environment=self.environment,
        )
        headers = (
            ("runtime-schema-version", "1"),
            ("event-id", str(command.command_id)),
            ("application-id", str(command.identity.application_id)),
            ("revision-id", str(command.identity.revision_id)),
            ("deployment-id", str(command.identity.deployment_id)),
            ("run-id", str(command.identity.run_id)),
            ("content-type", "application/json"),
        )
        self.messages.append(
            KafkaMessage(topic, PartitionKey.for_command(command), command, headers)
        )


class KafkaRunCommandQueue(RunCommandQueue):
    """In-memory equivalent of worker-owned Kafka topics.

    This queue is a test/local adapter only. There is no dispatcher component;
    commands are published directly into a priority heap and consumed by workers.
    """

    def __init__(
        self,
        transport: InMemoryKafkaTransport | None = None,
        *,
        lease_seconds: int = 60,
    ) -> None:
        self.transport = transport or InMemoryKafkaTransport()
        self._items: list[tuple[int, int, RunCommand, int]] = []
        self._sequence = 0
        self._leases: dict[str, RunCommandReceipt] = {}
        self._lease_seconds = lease_seconds
        self.dead_letters: list[RunCommand] = []

    async def publish(self, command: RunCommand) -> None:
        await self.transport.publish(command)
        self._push(command, delivery_count=0)

    async def receive(self, worker_id: WorkerId) -> RunCommandReceipt:
        del worker_id
        if not self._items:
            raise RuntimeFailure(ErrorCode.QUEUE_CLOSED, "no command is available")
        _priority, _sequence, command, delivery_count = heapq.heappop(self._items)
        now = datetime.now(UTC)
        receipt = RunCommandReceipt(
            command,
            LeaseId.new(),
            delivery_count + 1,
            now,
            now + timedelta(seconds=self._lease_seconds),
        )
        self._leases[str(receipt.lease_id)] = receipt
        return receipt

    async def acknowledge(self, receipt: RunCommandReceipt) -> None:
        if self._leases.pop(str(receipt.lease_id), None) is None:
            raise RuntimeFailure(ErrorCode.INVALID_EXECUTION_INPUT, "receipt is not active")

    async def reject(self, receipt: RunCommandReceipt, *, retryable: bool) -> None:
        if self._leases.pop(str(receipt.lease_id), None) is None:
            raise RuntimeFailure(ErrorCode.INVALID_EXECUTION_INPUT, "receipt is not active")
        if retryable and receipt.delivery_count < 3:
            self._push(receipt.command, delivery_count=receipt.delivery_count)
        else:
            self.dead_letters.append(receipt.command)

    def _push(self, command: RunCommand, *, delivery_count: int) -> None:
        heapq.heappush(
            self._items,
            (-int(command.priority), self._sequence, command, delivery_count),
        )
        self._sequence += 1
