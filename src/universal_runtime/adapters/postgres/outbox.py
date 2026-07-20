from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from universal_runtime.adapters.postgres.models import OutboxEventRow
from universal_runtime.domain.primitives.json_types import JsonObject
from universal_runtime.ports.queue import RunCommandQueue
from universal_runtime.transport.queue_codec import run_command_from_document


class PostgresRunCommandOutboxRelay:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        queue: RunCommandQueue,
        *,
        batch_size: int = 100,
    ) -> None:
        if batch_size < 1:
            raise ValueError("outbox batch_size must be positive")
        self._sessions = sessions
        self._queue = queue
        self._batch_size = batch_size

    async def publish_batch(self) -> int:
        published = 0
        async with self._sessions() as session:
            async with session.begin():
                rows = (
                    await session.execute(
                        select(OutboxEventRow)
                        .where(
                            OutboxEventRow.aggregate_type == "run_command",
                            OutboxEventRow.published_at.is_(None),
                        )
                        .order_by(OutboxEventRow.created_at.asc())
                        .with_for_update(skip_locked=True)
                        .limit(self._batch_size)
                    )
                ).scalars().all()
                for row in rows:
                    command = run_command_from_document(
                        cast(JsonObject, row.payload)
                    )
                    await self._queue.publish(command)
                    row.published_at = datetime.now(UTC)
                    published += 1
        return published
