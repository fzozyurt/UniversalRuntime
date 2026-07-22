from __future__ import annotations

import asyncio
import importlib
import os
import signal
from typing import Any

import uvicorn

from universal_runtime.adapters.grpc import WorkerServer
from universal_runtime.adapters.langgraph import LangGraphAdapter
from universal_runtime.adapters.langgraph.persistence import postgres_persistence
from universal_runtime.adapters.postgres.database import create_engine
from universal_runtime.adapters.postgres.langgraph import managed_langgraph_persistence
from universal_runtime.bootstrap.runtime_config import LauncherConfig
from universal_runtime.services.dispatcher.main import Dispatcher
from universal_runtime.services.gateway.app import create_app


async def _serve(config: LauncherConfig) -> None:
    """Run Gateway, Dispatcher and Worker in one process.

    This is a compact deployment profile, not a local/in-memory shortcut.  It
    retains Kafka, PostgreSQL and the gRPC worker boundary, and can be scaled by
    running multiple identical ``all`` pods with distinct instance IDs.
    """
    from universal_runtime.telemetry import init_observability

    init_observability()

    if not os.environ.get("UR_WORKER_TARGETS"):
        os.environ["UR_WORKER_TARGETS"] = "127.0.0.1:" + str(config.grpc_port)
    if not os.environ.get("UR_GATEWAY_REGISTER_URL"):
        os.environ["UR_GATEWAY_REGISTER_URL"] = (
            f"http://127.0.0.1:{config.port}/internal/workers/register"
        )
    if not os.environ.get("UR_WORKER_ADVERTISE_TARGET"):
        os.environ["UR_WORKER_ADVERTISE_TARGET"] = f"127.0.0.1:{config.grpc_port}"

    app = create_app()
    uvicorn_config = uvicorn.Config(
        app,
        host=config.host,
        port=config.port,
        timeout_keep_alive=75,
        timeout_graceful_shutdown=int(config.worker_drain_timeout_seconds),
        log_config=None,
    )
    http_server = uvicorn.Server(uvicorn_config)
    http_task = asyncio.create_task(http_server.serve())

    module_name, attribute = os.environ["UR_APPLICATION_ENTRYPOINT"].split(":", 1)
    target = getattr(importlib.import_module(module_name), attribute)
    database_url = os.environ["UR_DATABASE_URL"]
    engine = create_engine(database_url, pool_size=30, max_overflow=10)
    context = managed_langgraph_persistence(
        database_url,
        migration_engine=engine,
        application_id=os.environ.get("UR_APPLICATION_ID", "default"),
        environment=os.environ.get("UR_KAFKA_ENVIRONMENT", "local"),
        workspace_key=os.environ.get("UR_WORKSPACE_KEY", "default"),
        application_key=os.environ.get("UR_APPLICATION_ID", "default"),
    )
    persistence = await context.__aenter__()
    adapter = LangGraphAdapter(
        target,
        persistence_mode="platform-managed",
        providers=postgres_persistence(persistence.checkpointer, persistence.store),
    )

    async def _migrate_app(_request: Any) -> None:
        from sqlalchemy import text

        async with engine.connect() as _conn:
            await _conn.execute(text("SELECT 1"))
            await _conn.commit()

    worker = WorkerServer.create(
        configured_concurrency=config.worker_max_concurrency,
        policy_ceiling=int(os.getenv("UR_WORKER_POLICY_CEILING", "64")),
        adapter=adapter,
        migrate_app=_migrate_app,
    )
    dispatcher = Dispatcher()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, stop.set)
    loop.add_signal_handler(signal.SIGINT, stop.set)
    try:
        await worker.start_listening("127.0.0.1", config.grpc_port)
        dispatch_task = asyncio.create_task(dispatcher.run(stop))
        await stop.wait()
        dispatch_task.cancel()
        await worker.stop(config.worker_drain_timeout_seconds)
        http_server.should_exit = True
        await http_task
    finally:
        await dispatcher.close()
        await context.__aexit__(None, None, None)
        await engine.dispose()


def main(*, run_forever: bool = True) -> int:
    config = LauncherConfig.from_environment()
    config.validate()
    if run_forever:
        asyncio.run(_serve(config))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
