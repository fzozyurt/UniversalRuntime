from __future__ import annotations

import asyncio
import importlib
import os
import signal

from universal_runtime.adapters.grpc import WorkerServer
from universal_runtime.adapters.langgraph import LangGraphAdapter
from universal_runtime.adapters.langgraph.persistence import postgres_persistence
from universal_runtime.adapters.postgres.database import create_engine
from universal_runtime.adapters.postgres.langgraph import managed_langgraph_persistence
from universal_runtime.bootstrap.runtime_config import LauncherConfig
from universal_runtime.services.worker.registration import WorkerRegistrationPublisher


def create_server(adapter: object | None = None) -> WorkerServer:
    config = LauncherConfig.from_environment()
    return WorkerServer.create(
        configured_concurrency=int(
            os.getenv(
                "UR_WORKER_CONFIGURED_CONCURRENCY",
                str(config.worker_max_concurrency),
            )
        ),
        policy_ceiling=int(os.getenv("UR_WORKER_POLICY_CEILING", "64")),
        adapter=adapter,
    )


def main(*, run_forever: bool = True) -> int:
    config = LauncherConfig.from_environment()
    config.validate()
    if run_forever:
        asyncio.run(_serve(config))
    return 0


async def _serve(config: LauncherConfig) -> None:
    entrypoints = _entrypoints()
    database_url = os.environ.get("UR_STATE_DATABASE_URL") or os.environ[
        "UR_DATABASE_URL"
    ]
    engine = create_engine(
        database_url,
        pool_size=int(os.environ.get("UR_WORKER_DB_POOL_SIZE", "30")),
        max_overflow=int(os.environ.get("UR_WORKER_DB_MAX_OVERFLOW", "10")),
    )
    persistence_context = managed_langgraph_persistence(
        database_url,
        migration_engine=engine,
        application_id=os.environ.get("UR_APPLICATION_ID", "default"),
        environment=os.environ.get("UR_KAFKA_ENVIRONMENT", "local"),
        workspace_key=os.environ.get("UR_WORKSPACE_KEY", "default"),
        application_key=os.environ.get("UR_APPLICATION_ID", "default"),
    )
    persistence = await persistence_context.__aenter__()
    adapters: dict[str, LangGraphAdapter] = {}
    graph_entrypoints: dict[str, str] = {}
    for entrypoint in entrypoints:
        module_name, attribute = entrypoint.split(":", 1)
        target = getattr(importlib.import_module(module_name), attribute)
        adapter = LangGraphAdapter(
            target,
            persistence_mode="platform-managed",
            providers=postgres_persistence(
                persistence.checkpointer,
                persistence.store,
            ),
        )
        graph_id = adapter.descriptor.graph_id
        if graph_id in adapters:
            raise RuntimeError(
                f"duplicate graph_id in application image: {graph_id}"
            )
        adapters[graph_id] = adapter
        graph_entrypoints[graph_id] = entrypoint

    server = create_server(adapters)
    publisher = WorkerRegistrationPublisher(
        adapters,
        graph_entrypoints,
        server,
        config,
    )
    await server.start_listening(config.grpc_host, config.grpc_port)
    await publisher.publish(attempts=10)
    heartbeat_task = asyncio.create_task(publisher.heartbeat_loop())
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, stopped.set)
    loop.add_signal_handler(signal.SIGINT, stopped.set)
    try:
        await stopped.wait()
    finally:
        heartbeat_task.cancel()
        await asyncio.gather(heartbeat_task, return_exceptions=True)
        await publisher.publish_draining()
        await server.stop(config.worker_drain_timeout_seconds)
        await persistence_context.__aexit__(None, None, None)
        await engine.dispose()


def _entrypoints() -> tuple[str, ...]:
    raw = os.environ.get("UR_APPLICATION_ENTRYPOINTS") or os.environ.get(
        "UR_APPLICATION_ENTRYPOINT"
    )
    if not raw:
        raise RuntimeError(
            "UR_APPLICATION_ENTRYPOINT or UR_APPLICATION_ENTRYPOINTS is required"
        )
    return tuple(item.strip() for item in raw.split(",") if item.strip())


__all__ = ["main"]
