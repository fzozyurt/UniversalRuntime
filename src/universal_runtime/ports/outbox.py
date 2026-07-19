from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from universal_runtime.domain.identity import CommandId
from universal_runtime.domain.primitives.json_types import JsonObject


@dataclass(frozen=True, slots=True)
class OutboxMessage:
    message_id: CommandId
    topic: str
    key: str
    payload: JsonObject
    created_at: datetime
    published: bool = False


class OutboxRepository(Protocol):
    async def append(self, message: OutboxMessage) -> OutboxMessage: ...

    async def pending(self, *, limit: int = 100) -> tuple[OutboxMessage, ...]: ...

    async def mark_published(self, message_id: CommandId) -> None: ...
