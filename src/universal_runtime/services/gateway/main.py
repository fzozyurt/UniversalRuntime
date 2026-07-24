from __future__ import annotations

import os

import uvicorn
from fastapi import FastAPI

from universal_runtime.bootstrap.runtime_config import LauncherConfig
from universal_runtime.services.gateway.app import create_app
from universal_runtime.services.gateway.event_fanout import attach_runtime_event_fanout
from universal_runtime.services.gateway.worker_control import attach_worker_control


def create_gateway_app() -> FastAPI:
    app = create_app(
        custom_http_target=os.environ.get("UR_APPLICATION_HTTP_TARGET"),
    )
    attach_worker_control(app)
    attach_runtime_event_fanout(app)
    return app


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
