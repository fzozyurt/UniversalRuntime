from __future__ import annotations

import pytest
from tests.test_event_journal import identity

from universal_runtime.adapters.kafka import InMemoryKafkaTransport, KafkaRuntimeEventPublisher
from universal_runtime.adapters.memory.event_batching import EventBatchConfig, RuntimeEventBatcher
from universal_runtime.adapters.memory.events import InMemoryEventJournal
from universal_runtime.adapters.memory.projection import (
    CompositeProjectionSink,
    InMemoryProjectionSink,
    NullProjectionSink,
)
from universal_runtime.domain.events import RuntimeEventDraft, RuntimeEventType
from universal_runtime.services.event_projector.projector import LifecycleProjector


@pytest.mark.asyncio
async def test_batcher_flushes_by_count_and_terminal() -> None:
    written: list[tuple[object, ...]] = []

    async def write(batch: tuple[object, ...]) -> None:
        written.append(batch)

    batcher = RuntimeEventBatcher(write, EventBatchConfig(max_events=2, max_bytes=10000))
    ident = identity()
    journal = InMemoryEventJournal()
    first = await journal.append(RuntimeEventDraft(ident, RuntimeEventType.MESSAGE_DELTA, data="a"))
    second = await journal.append(
        RuntimeEventDraft(ident, RuntimeEventType.MESSAGE_DELTA, data="b")
    )
    terminal = await journal.append(
        RuntimeEventDraft(ident, RuntimeEventType.RUN_COMPLETED, data={"ok": True})
    )
    await batcher.add(first)
    await batcher.add(second)
    await batcher.add(terminal)
    assert [len(item) for item in written] == [2, 1]
    assert written[-1][0] == terminal


@pytest.mark.asyncio
async def test_projector_is_idempotent_and_rejects_out_of_order() -> None:
    ident = identity()
    journal = InMemoryEventJournal()
    sink = InMemoryProjectionSink()
    projector = LifecycleProjector(CompositeProjectionSink(sink, NullProjectionSink()))
    queued = await journal.append(RuntimeEventDraft(ident, RuntimeEventType.RUN_QUEUED))
    started = await journal.append(RuntimeEventDraft(ident, RuntimeEventType.RUN_STARTED))
    completed = await journal.append(
        RuntimeEventDraft(ident, RuntimeEventType.RUN_COMPLETED, data={"answer": 1})
    )
    assert await projector.handle(queued)
    assert await projector.handle(started)
    assert await projector.handle(completed)
    assert not await projector.handle(completed)
    projection = sink.projections[str(ident.run_id)]
    assert projection.status == "success"
    assert projection.result == {"answer": 1}


@pytest.mark.asyncio
async def test_projector_rejects_gap() -> None:
    ident = identity()
    journal = InMemoryEventJournal()
    await journal.append(RuntimeEventDraft(ident, RuntimeEventType.RUN_QUEUED))
    event = await journal.append(RuntimeEventDraft(ident, RuntimeEventType.RUN_STARTED))
    sink = InMemoryProjectionSink()
    projector = LifecycleProjector(sink)
    with pytest.raises(ValueError):
        await projector.handle(event)


@pytest.mark.asyncio
async def test_kafka_lifecycle_publisher_uses_partition_key_and_topic() -> None:
    transport = InMemoryKafkaTransport()
    publisher = KafkaRuntimeEventPublisher(transport)
    ident = identity()
    event = await InMemoryEventJournal().append(
        RuntimeEventDraft(ident, RuntimeEventType.RUN_STARTED)
    )
    await publisher.publish(event)
    assert transport.messages[0].topic == transport.topics.lifecycle
    assert transport.messages[0].key == "a:thread"
    assert ("event-id", str(event.event_id)) in transport.messages[0].headers
