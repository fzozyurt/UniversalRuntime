from __future__ import annotations

import asyncio
import os


def create_server() -> object:
    from universal_runtime.adapters.grpc import WorkerServer

    configured = int(os.getenv("UR_WORKER_CONFIGURED_CONCURRENCY", "8"))
    policy = int(os.getenv("UR_WORKER_POLICY_CEILING", "64"))
    return WorkerServer.create(configured_concurrency=configured, policy_ceiling=policy)


def main(*, run_forever: bool = False) -> int:
    from universal_runtime.bootstrap.runtime_config import LauncherConfig

    config = LauncherConfig.from_environment()
    config.validate()
    if run_forever:
        from universal_runtime.adapters.grpc import WorkerServer

        server = WorkerServer.create(
            configured_concurrency=int(
                os.getenv("UR_WORKER_CONFIGURED_CONCURRENCY", str(config.worker_max_concurrency))
            ),
            policy_ceiling=int(os.getenv("UR_WORKER_POLICY_CEILING", "64")),
        )
        asyncio.run(server.start(config.grpc_host, config.grpc_port))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
