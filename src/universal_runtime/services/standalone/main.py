from __future__ import annotations

import asyncio
import importlib
import os
import signal

import uvicorn

from universal_runtime.adapters.grpc import WorkerServer
from universal_runtime.adapters.langgraph import LangGraphAdapter
from universal_runtime.adapters.langgraph.persistence import postgres_persistence
from universal_runtime.adapters.postgres.database import create_engine
from universal_runtime.adapters.postgres.langgraph import managed_langgraph_persistence
from universal_runtime.bootstrap.runtime_config import LauncherConfig
from universal_runtime.services.dispatcher.main import Dispatcher
from universal_runtime.services.gateway.app import create_app
from universal_runtime.services.gateway.shared import attach_postgres_control_plane
from universal_runtime.services.outbox_relay.main import OutboxRelayService
from universal_runtime.services.worker.registration import WorkerRegistrationPublisher


def _entrypoints() -> tuple[str, ...]:
    raw = os.environ.get("UR_APPLICATION_ENTRYPOINTS") or os.environ.get(
        "UR_APPLICATION_ENTRYPOINT"
    )
    if not raw:
        raise RuntimeError(
            "standalone mode requires an application graph entrypoint"
        )
    return tuple(item.strip() for item in raw.split(",") if item.strip())


async def _serve(config: LauncherConfig) -> None:
    """Run API, outbox relay, dispatcher and one application worker together."""
    os.environ.setdefault(
        "UR_GATEWAY_REGISTER_URL",
        f"http://127.0.0.1:{config.port}/internal/workers/register",
    )
    os.environ.setdefault(
        "UR_WORKER_ADVERTISE_TARGET",
        f"127.0.0.1:{config.grpc_port}",
    )
    os.environ.setdefault("UR_ACTIVATE_REVISION", "true")

    application = attach_postgres_control_plane(create_app())
    http_server = uvicorn.Server(
        uvicorn.Config(
            application,
            host=config.host,
            port=config.port,
            timeout_keep_alive=75,
            timeout_graceful_shutdown=int(
                config.worker_drain_timeout_seconds
            ),
            log_config=None,
        )
    )
    http_task = asyncio.create_task(http_server.serve())

    database_url = os.environ["UR_DATABASE_URL"]
    engine = create_engine(database_url, pool_size=30, max_overflow=10)
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
    for entrypoint in _entrypoints():
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

    worker = WorkerServer.create(
        configured_concurrency=config.worker_max_concurrency,
        policy_ceiling=int(os.getenv("UR_WORKER_POLICY_CEILING", "64")),
        adapter=adapters,
    )
    publisher = WorkerRegistrationPublisher(
        adapters,
        graph_entrypoints,
        worker,
        config,
    )
    dispatcher = Dispatcher()
    outbox_relay = OutboxRelayService()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, stop.set)
    loop.add_signal_handler(signal.SIGINT, stop.set)
    tasks: list[asyncio.Task[None]] = []
    try:
        await worker.start_listening("127.0.0.1", config.grpc_port)
        await publisher.publish(attempts=10)
        tasks = [
            asyncio.create_task(publisher.heartbeat_loop()),
            asyncio.create_task(dispatcher.run(stop)),
            asyncio.create_task(outbox_relay.run(stop)),
        ]
        await stop.wait()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await publisher.publish_draining()
        await worker.stop(config.worker_drain_timeout_seconds)
        http_server.should_exit = True
        await http_task
        await dispatcher.close()
        await outbox_relay.close()
        await persistence_context.__aexit__(None, None, None)
        await engine.dispose()


def main(*, run_forever: bool = True) -> int:
    config = LauncherConfig.from_environment()
    config.validate()
    if run_forever:
        asyncio.run(_serve(config))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
