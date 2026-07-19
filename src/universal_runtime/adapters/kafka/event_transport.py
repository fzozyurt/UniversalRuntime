from __future__ import annotations

from universal_runtime.adapters.kafka.dispatcher import InMemoryKafkaTransport, KafkaMessage
from universal_runtime.adapters.kafka.topics import TopicNames
from universal_runtime.domain.events import RuntimeEvent
from universal_runtime.ports.events import RuntimeEventPublisher


class KafkaRuntimeEventPublisher(RuntimeEventPublisher):
    def __init__(self, transport: InMemoryKafkaTransport, topics: TopicNames | None = None) -> None:
        self._transport = transport
        self._topics = topics or transport.topics

    async def publish(self, event: RuntimeEvent) -> None:
        identity = event.identity
        key = f"{identity.scope.application_id}:{identity.thread_id or identity.run_id}"
        topic = (
            self._topics.lifecycle
            if str(event.type).startswith(("run.", "attempt."))
            else self._topics.execution_events
        )
        headers = (
            ("runtime-schema-version", "1"),
            ("event-id", str(event.event_id)),
            ("run-id", str(identity.run_id)),
            ("content-type", "application/json"),
        )
        self._transport.messages.append(KafkaMessage(topic, key, event, headers))
