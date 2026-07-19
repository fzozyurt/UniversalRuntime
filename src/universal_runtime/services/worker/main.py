from __future__ import annotations

import asyncio
import os
import signal

from universal_runtime.adapters.grpc import WorkerServer
from universal_runtime.bootstrap.runtime_config import LauncherConfig


def create_server() -> WorkerServer:
    config = LauncherConfig.from_environment()
    return WorkerServer.create(
        configured_concurrency=int(
            os.getenv("UR_WORKER_CONFIGURED_CONCURRENCY", str(config.worker_max_concurrency))
        ),
        policy_ceiling=int(os.getenv("UR_WORKER_POLICY_CEILING", "64")),
    )


def main(*, run_forever: bool = True) -> int:
    config = LauncherConfig.from_environment()
    config.validate()
    if run_forever:
        asyncio.run(_serve(config))
    return 0


async def _serve(config: LauncherConfig) -> None:
    server = create_server()
    await server.start_listening(config.grpc_host, config.grpc_port)
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, stopped.set)
    loop.add_signal_handler(signal.SIGINT, stopped.set)
    await stopped.wait()
    await server.stop(config.worker_drain_timeout_seconds)


__all__ = ["main"]
