from __future__ import annotations

import uvicorn

from universal_runtime.bootstrap.runtime_config import LauncherConfig
from universal_runtime.services.gateway.shared import create_shared_gateway_app


def main(*, run_forever: bool = True) -> int:
    config = LauncherConfig.from_environment()
    config.validate()
    if run_forever:
        uvicorn.run(
            create_shared_gateway_app(),
            host=config.host,
            port=config.port,
            timeout_keep_alive=75,
            timeout_graceful_shutdown=int(config.worker_drain_timeout_seconds),
        )
    return 0


app = create_shared_gateway_app()


if __name__ == "__main__":
    raise SystemExit(main())
