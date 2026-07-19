from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import replace

from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.domain.identity import CommandId
from universal_runtime.ports.outbox import OutboxMessage


class InMemoryOutboxRepository:
    def __init__(self) -> None:
        self._messages: dict[str, OutboxMessage] = {}
        self._lock = asyncio.Lock()

    async def append(self, message: OutboxMessage) -> OutboxMessage:
        async with self._lock:
            key = str(message.message_id)
            if key in self._messages:
                raise RuntimeFailure(ErrorCode.INVALID_EXECUTION_INPUT, "outbox message exists")
            self._messages[key] = message
            return deepcopy(message)

    async def pending(self, *, limit: int = 100) -> tuple[OutboxMessage, ...]:
        async with self._lock:
            values = [item for item in self._messages.values() if not item.published]
            return tuple(deepcopy(values[:limit]))

    async def mark_published(self, message_id: CommandId) -> None:
        async with self._lock:
            key = str(message_id)
            if key not in self._messages:
                raise RuntimeFailure(ErrorCode.RESOURCE_NOT_FOUND, "outbox message not found")
            self._messages[key] = replace(self._messages[key], published=True)
