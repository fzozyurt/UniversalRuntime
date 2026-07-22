from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.domain.execution import RunCommand, RunCommandReceipt
from universal_runtime.domain.identity import LeaseId, WorkerId
from universal_runtime.ports.queue import RunCommandQueue

from .topics import TopicNames


@dataclass(frozen=True, slots=True)
class KafkaMessage:
    topic: str
    key: str
    value: object
    headers: tuple[tuple[str, str], ...]


class PartitionKey:
    @staticmethod
    def for_command(command: RunCommand) -> str:
        identity = command.identity
        lineage = identity.thread_id or identity.run_id
        return f"{identity.application_id}:{lineage}"


class InMemoryKafkaTransport:
    """Deterministic Kafka-shaped transport used by local and unit tests."""

    def __init__(self, topics: TopicNames | None = None) -> None:
        self.topics = topics or TopicNames.from_config()
        self.messages: list[KafkaMessage] = []

    async def publish(self, command: RunCommand) -> None:
        topic = TopicNames.run_topic_for(
            "rt", command.identity.application_id, int(command.priority)
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


class WeightedFairDispatcher:
    """Priority scheduler with weighted fairness and age promotion.

    The scheduler only selects commands. Commit/acknowledgement stays with the caller,
    after authoritative execution persistence succeeds.
    """

    def __init__(
        self,
        *,
        weights: tuple[int, int, int] = (8, 3, 1),
        age_after: timedelta = timedelta(seconds=30),
    ) -> None:
        if len(weights) != 3 or any(weight <= 0 for weight in weights):
            raise ValueError("weights must contain three positive values")
        self._weights = weights
        self._age_after = age_after
        self._items: list[tuple[int, int, RunCommand, int]] = []
        self._sequence = 0
        self._cursor = 0

    def add(self, command: RunCommand, *, delivery_count: int = 0) -> None:
        self._items.append((int(command.priority), self._sequence, command, delivery_count))
        self._sequence += 1

    def choose(self, *, now: datetime | None = None) -> tuple[RunCommand, int] | None:
        if not self._items:
            return None
        current = now or datetime.now(UTC)
        aged = [item for item in self._items if current - item[2].created_at >= self._age_after]
        if aged:
            selected = min(aged, key=lambda item: item[1])
        else:
            priority = (100, 50, 10)[self._cursor % 3]
            candidates = [item for item in self._items if item[0] == priority]
            if not candidates:
                candidates = [
                    item for item in self._items if item[0] == max(x[0] for x in self._items)
                ]
            selected = min(candidates, key=lambda item: item[1])
            self._cursor += 1
        self._items.remove(selected)
        return selected[2], selected[3]


class KafkaRunCommandQueue(RunCommandQueue):
    """Lease/receipt queue facade; a Kafka client can be supplied by composition root."""

    def __init__(
        self, transport: InMemoryKafkaTransport | None = None, *, lease_seconds: int = 60
    ) -> None:
        self.transport = transport or InMemoryKafkaTransport()
        self.dispatcher = WeightedFairDispatcher()
        self._leases: dict[str, RunCommandReceipt] = {}
        self._lease_seconds = lease_seconds
        self.dead_letters: list[RunCommand] = []

    async def publish(self, command: RunCommand) -> None:
        await self.transport.publish(command)
        self.dispatcher.add(command)

    async def receive(self, worker_id: WorkerId) -> RunCommandReceipt:
        selected = self.dispatcher.choose()
        if selected is None:
            raise RuntimeFailure(ErrorCode.QUEUE_CLOSED, "no command is available")
        command, delivery_count = selected
        now = datetime.now(UTC)
        receipt = RunCommandReceipt(
            command,
            LeaseId.new(),
            delivery_count + 1,
            now,
            now + timedelta(seconds=self._lease_seconds),
        )
        self._leases[str(receipt.lease_id)] = receipt
        del worker_id
        return receipt

    async def acknowledge(self, receipt: RunCommandReceipt) -> None:
        if self._leases.pop(str(receipt.lease_id), None) is None:
            raise RuntimeFailure(ErrorCode.INVALID_EXECUTION_INPUT, "receipt is not active")

    async def reject(self, receipt: RunCommandReceipt, *, retryable: bool) -> None:
        if self._leases.pop(str(receipt.lease_id), None) is None:
            raise RuntimeFailure(ErrorCode.INVALID_EXECUTION_INPUT, "receipt is not active")
        if retryable and receipt.delivery_count < 3:
            self.dispatcher.add(receipt.command, delivery_count=receipt.delivery_count)
        else:
            self.dead_letters.append(receipt.command)
