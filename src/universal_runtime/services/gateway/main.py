from __future__ import annotations

import uvicorn

from universal_runtime.bootstrap.runtime_config import LauncherConfig
from universal_runtime.services.gateway.app import create_app
from universal_runtime.services.gateway.worker_control import attach_worker_control


def create_gateway_app() -> object:
    return attach_worker_control(create_app())


def main(*, run_forever: bool = True) -> int:
    config = LauncherConfig.from_environment()
    config.validate()
    if run_forever:
        from universal_runtime.telemetry import init_observability

        init_observability()
        uvicorn.run(
            create_gateway_app(),
            host=config.host,
            port=config.port,
            timeout_keep_alive=75,
            timeout_graceful_shutdown=int(config.worker_drain_timeout_seconds),
        )
    return 0


app = create_gateway_app()


if __name__ == "__main__":
    raise SystemExit(main())
