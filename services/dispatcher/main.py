from __future__ import annotations

import asyncio

from universal_runtime.adapters.kafka import KafkaRunCommandQueue, TopicNames
from universal_runtime.bootstrap.runtime_config import LauncherConfig


def create_dispatch_queue() -> KafkaRunCommandQueue:
    return KafkaRunCommandQueue()


def main(*, run_forever: bool = False) -> int:
    config = LauncherConfig.from_environment()
    config.validate()
    TopicNames.from_config(prefix=config.topic_prefix, environment=config.kafka_environment)
    if run_forever:
        asyncio.run(_serve())
    return 0


async def _serve() -> None:
    stop = asyncio.Event()
    await stop.wait()


if __name__ == "__main__":
    raise SystemExit(main())
