from __future__ import annotations

import asyncio
import logging
import os
import signal

from universal_runtime.adapters.kafka import AioKafkaRunCommandQueue, TopicNames
from universal_runtime.adapters.postgres.database import create_engine, create_session_factory
from universal_runtime.adapters.postgres.outbox import PostgresRunCommandOutboxRelay
from universal_runtime.bootstrap.runtime_config import LauncherConfig

_LOGGER = logging.getLogger(__name__)


class OutboxRelayService:
    def __init__(self) -> None:
        self.config = LauncherConfig.from_environment()
        database_url = os.environ.get("UR_PLATFORM_DATABASE_URL") or os.environ["UR_DATABASE_URL"]
        self.engine = create_engine(
            database_url,
            pool_size=int(os.environ.get("UR_OUTBOX_DB_POOL_SIZE", "5")),
            max_overflow=int(os.environ.get("UR_OUTBOX_DB_MAX_OVERFLOW", "5")),
        )
        sessions = create_session_factory(self.engine)
        self.queue = AioKafkaRunCommandQueue(
            bootstrap_servers=self.config.kafka_bootstrap_servers,
            topics=TopicNames.from_config(
                prefix=self.config.topic_prefix,
                environment=self.config.kafka_environment,
            ),
            group_id=os.environ.get("UR_OUTBOX_GROUP_ID", "runtime.outbox-relay"),
        )
        self.relay = PostgresRunCommandOutboxRelay(
            sessions,
            self.queue,
            batch_size=max(1, int(os.environ.get("UR_OUTBOX_BATCH_SIZE", "100"))),
        )
        self.poll_seconds = max(
            0.01,
            float(os.environ.get("UR_OUTBOX_POLL_SECONDS", "0.1")),
        )

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                published = await self.relay.publish_batch()
                if published == 0:
                    await asyncio.sleep(self.poll_seconds)
            except asyncio.CancelledError:
                raise
            except Exception:
                _LOGGER.exception("outbox relay iteration failed")
                await asyncio.sleep(self.poll_seconds)

    async def close(self) -> None:
        await self.queue.close()
        await self.engine.dispose()


async def _serve() -> None:
    service = OutboxRelayService()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, stop.set)
    loop.add_signal_handler(signal.SIGINT, stop.set)
    try:
        await service.run(stop)
    finally:
        await service.close()


def main(*, run_forever: bool = False) -> int:
    config = LauncherConfig.from_environment()
    config.validate()
    if run_forever:
        asyncio.run(_serve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main(run_forever=True))
