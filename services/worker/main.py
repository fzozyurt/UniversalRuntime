from __future__ import annotations

import os


def create_server() -> object:
    from universal_runtime.adapters.grpc import WorkerServer

    configured = int(os.getenv("UR_WORKER_CONFIGURED_CONCURRENCY", "8"))
    policy = int(os.getenv("UR_WORKER_POLICY_CEILING", "64"))
    return WorkerServer.create(configured_concurrency=configured, policy_ceiling=policy)


def main() -> int:
    # The process launcher owns asyncio/grpc lifecycle; construction is exposed separately.
    int(os.getenv("UR_WORKER_CONFIGURED_CONCURRENCY", "8"))
    int(os.getenv("UR_WORKER_POLICY_CEILING", "64"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
