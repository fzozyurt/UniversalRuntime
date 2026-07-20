from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition

from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.domain.execution import QueuePriority, RunCommand, RunCommandReceipt
from universal_runtime.domain.identity import LeaseId, WorkerId
from universal_runtime.domain.primitives.json_types import JsonObject
from universal_runtime.ports.queue import RunCommandQueue
from universal_runtime.transport.queue_codec import (
    run_command_from_document,
    run_command_to_document,
)

from .dispatcher import PartitionKey
from .topics import TopicNames


def _json_command(command: RunCommand) -> bytes:
    return json.dumps(
        run_command_to_document(command),
        separators=(",", ":"),
    ).encode()


def _command(payload: bytes) -> RunCommand:
    value = json.loads(payload)
    if not isinstance(value, dict):
        raise RuntimeFailure(
            ErrorCode.INVALID_EXECUTION_INPUT,
            "Kafka run command payload must be a JSON object",
        )
    return run_command_from_document(cast(JsonObject, value))


class AioKafkaRunCommandQueue(RunCommandQueue):
    """Durable production command queue backed by Kafka consumer groups."""

    def __init__(self, *, bootstrap_servers: str, topics: TopicNames, group_id: str) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._topics = topics
        self._group_id = group_id
        self._producer: AIOKafkaProducer | None = None
        self._consumer: AIOKafkaConsumer | None = None
        self._leases: dict[str, Any] = {}
        self._delivery_counts: dict[tuple[str, int, int], int] = {}
        self._lease_seconds = 60

    async def _start(self, *, consumer: bool) -> None:
        if self._producer is None:
            self._producer = AIOKafkaProducer(bootstrap_servers=self._bootstrap_servers)
            await self._producer.start()
        if consumer and self._consumer is None:
            instance = AIOKafkaConsumer(
                self._topics.interactive,
                self._topics.normal,
                self._topics.batch,
                bootstrap_servers=self._bootstrap_servers,
                group_id=self._group_id,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
            )
            try:
                await instance.start()
            except Exception:
                await instance.stop()
                raise
            self._consumer = instance

    async def publish(self, command: RunCommand) -> None:
        await self._start(consumer=False)
        assert self._producer is not None
        topic = {
            QueuePriority.INTERACTIVE: self._topics.interactive,
            QueuePriority.NORMAL: self._topics.normal,
            QueuePriority.BATCH: self._topics.batch,
        }[command.priority]
        await self._producer.send_and_wait(
            topic,
            _json_command(command),
            key=PartitionKey.for_command(command).encode(),
            headers=[
                ("runtime-schema-version", b"1"),
                ("run-id", str(command.identity.run_id).encode()),
                ("graph-id", command.request.target.graph_id.encode()),
            ],
        )

    @staticmethod
    def _message_key(message: Any) -> tuple[str, int, int]:
        return message.topic, int(message.partition), int(message.offset)

    async def receive(self, worker_id: WorkerId) -> RunCommandReceipt:
        del worker_id
        await self._start(consumer=True)
        assert self._consumer is not None
        message = await self._consumer.getone()
        command = _command(message.value)
        now = datetime.now(UTC)
        message_key = self._message_key(message)
        delivery_count = self._delivery_counts.get(message_key, 0) + 1
        self._delivery_counts[message_key] = delivery_count
        receipt = RunCommandReceipt(
            command,
            LeaseId.new(),
            delivery_count,
            now,
            now + timedelta(seconds=self._lease_seconds),
        )
        self._leases[str(receipt.lease_id)] = message
        return receipt

    async def acknowledge(self, receipt: RunCommandReceipt) -> None:
        message = self._leases.pop(str(receipt.lease_id), None)
        if message is None or self._consumer is None:
            raise RuntimeFailure(
                ErrorCode.INVALID_EXECUTION_INPUT,
                "receipt is not active",
            )
        self._delivery_counts.pop(self._message_key(message), None)
        await self._consumer.commit(
            {TopicPartition(message.topic, message.partition): message.offset + 1}
        )

    async def reject(self, receipt: RunCommandReceipt, *, retryable: bool) -> None:
        message = self._leases.pop(str(receipt.lease_id), None)
        if message is None or self._consumer is None:
            raise RuntimeFailure(
                ErrorCode.INVALID_EXECUTION_INPUT,
                "receipt is not active",
            )
        partition = TopicPartition(message.topic, message.partition)
        if retryable:
            # Reset the consumer position to the same record. The offset remains
            # uncommitted, preserving at-least-once delivery after rebalance/restart.
            self._consumer.seek(partition, message.offset)
            return
        self._delivery_counts.pop(self._message_key(message), None)
        await self._consumer.commit({partition: message.offset + 1})

    async def close(self) -> None:
        consumer, producer = self._consumer, self._producer
        self._consumer = self._producer = None
        self._leases.clear()
        self._delivery_counts.clear()
        if consumer is not None:
            await consumer.stop()
        if producer is not None:
            await producer.stop()
