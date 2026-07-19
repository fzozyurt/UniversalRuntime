from __future__ import annotations

import asyncio

from universal_runtime.bootstrap.runtime_config import LauncherConfig


def main(*, run_forever: bool = True) -> int:
    """Run the projector process until the orchestrator sends SIGTERM."""
    config = LauncherConfig.from_environment()
    config.validate()
    if run_forever:
        asyncio.run(_serve())
    return 0


async def _serve() -> None:
    stop = asyncio.Event()
    await stop.wait()
