from __future__ import annotations

import asyncio

import pytest
from tests.test_event_journal import identity

from universal_runtime.adapters.memory.events import InMemoryEventJournal
from universal_runtime.domain.errors import RuntimeFailure
from universal_runtime.domain.events import RuntimeEventDraft, RuntimeEventType


@pytest.mark.asyncio
async def test_slow_subscriber_isolated_from_append_and_other_subscriber() -> None:
    journal = InMemoryEventJournal(queue_size=1)
    ident = identity()
    slow = journal.subscribe(ident.run_id)
    fast = journal.subscribe(ident.run_id)
    slow_task = asyncio.create_task(slow.__anext__())
    fast_task = asyncio.create_task(fast.__anext__())
    await asyncio.sleep(0)
    await journal.append(RuntimeEventDraft(ident, RuntimeEventType.RUN_STARTED))
    assert (await fast_task).sequence == 0
    await journal.append(RuntimeEventDraft(ident, RuntimeEventType.RUN_COMPLETED))
    assert (await journal.replay(ident.run_id))[-1].sequence == 1
    assert (await slow_task).sequence == 0
    await slow.aclose()
    await fast.aclose()


@pytest.mark.asyncio
async def test_negative_cursor_is_typed_error() -> None:
    journal = InMemoryEventJournal()
    with pytest.raises(RuntimeFailure):
        await journal.replay(identity().run_id, after_sequence=-2)
