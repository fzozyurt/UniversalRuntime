from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from universal_runtime.adapters.grpc.generated.runtime.v1 import execution_pb2

from .sasl_config import kafka_sasl_kwargs
from .topics import TopicNames

RuntimeEventHandler = Callable[[Any], Awaitable[None]]


class AioKafkaRuntimeEventPublisher:
    """Publish transient Runtime events without duplicating framework history."""

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        prefix: str,
        environment: str,
        application_id: str,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._topic = TopicNames.from_config(
            prefix=prefix,
            environment=environment,
            application_id=application_id,
        ).execution_events
        self._producer: AIOKafkaProducer | None = None

    async def publish(self, event: Any) -> None:
        if self._producer is None:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self._bootstrap_servers,
                enable_idempotence=True,
                **kafka_sasl_kwargs(),
            )
            await self._producer.start()
        affinity = event.identity.thread_id or event.identity.run_id
        await self._producer.send_and_wait(
            self._topic,
            event.SerializeToString(),
            key=f"{event.identity.application_id}:{affinity}".encode(),
            headers=[
                ("runtime-schema-version", b"1"),
                ("content-type", b"application/x-protobuf"),
                ("run-id", event.identity.run_id.encode()),
            ],
        )

    async def close(self) -> None:
        producer = self._producer
        self._producer = None
        if producer is not None:
            await producer.stop()


class AioKafkaRuntimeEventSubscriber:
    """Broadcast one application event topic into a single Gateway replica."""

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        prefix: str,
        environment: str,
        application_id: str,
        gateway_instance_id: str,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._topic = TopicNames.from_config(
            prefix=prefix,
            environment=environment,
            application_id=application_id,
        ).execution_events
        # Every Gateway gets its own group so live events are fanned out to all
        # replicas. Kafka is a transient stream transport here, not a history DB.
        self._group_id = (
            f"rt.{environment}.{application_id}.gateway-events.{gateway_instance_id}.v1"
        )
        self._consumer: AIOKafkaConsumer | None = None

    async def run(self, handler: RuntimeEventHandler) -> None:
        consumer = AIOKafkaConsumer(
            self._topic,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            enable_auto_commit=True,
            auto_offset_reset="latest",
            **kafka_sasl_kwargs(),
        )
        await consumer.start()
        self._consumer = consumer
        try:
            async for message in consumer:
                event = execution_pb2.RuntimeEvent()
                event.ParseFromString(message.value)
                await handler(event)
        finally:
            self._consumer = None
            await consumer.stop()

    async def close(self) -> None:
        consumer = self._consumer
        self._consumer = None
        if consumer is not None:
            await consumer.stop()
