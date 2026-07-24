from __future__ import annotations

import asyncio
import importlib
import os
import signal
from typing import Any

import uvicorn

from universal_runtime.adapters.grpc import WorkerServer
from universal_runtime.adapters.kafka import AioKafkaRunCommandQueue
from universal_runtime.adapters.langgraph import LangGraphAdapter
from universal_runtime.adapters.langgraph.persistence import postgres_persistence
from universal_runtime.adapters.postgres.database import create_engine, create_session_factory
from universal_runtime.adapters.postgres.langgraph import managed_langgraph_persistence
from universal_runtime.adapters.postgres.repositories import (
    PostgresRunRepository,
    PostgresThreadRepository,
)
from universal_runtime.bootstrap.runtime_config import LauncherConfig
from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.domain.identity import WorkerId
from universal_runtime.services.gateway.main import create_gateway_app
from universal_runtime.services.worker.execution import process_receipt
from universal_runtime.services.worker.migrations import create_application_migration_handler
from universal_runtime.services.worker.registration import heartbeat_gateway, register_with_gateway


async def _serve(config: LauncherConfig) -> None:
    """Run Gateway and Worker in one process using the production contracts."""

    from universal_runtime.telemetry import init_observability

    init_observability()
    os.environ.setdefault(
        "UR_GATEWAY_GRPC_TARGET",
        f"127.0.0.1:{os.environ.get('UR_GATEWAY_GRPC_PORT', '9091')}",
    )
    os.environ.setdefault(
        "UR_GATEWAY_CONTROL_GRPC_TARGET",
        f"127.0.0.1:{os.environ.get('UR_GATEWAY_CONTROL_GRPC_PORT', '9092')}",
    )
    os.environ.setdefault(
        "UR_WORKER_ADVERTISE_TARGET",
        f"127.0.0.1:{config.grpc_port}",
    )

    http_server = uvicorn.Server(
        uvicorn.Config(
            create_gateway_app(),
            host=config.host,
            port=config.port,
            timeout_keep_alive=75,
            timeout_graceful_shutdown=int(config.worker_drain_timeout_seconds),
            log_config=None,
        )
    )
    http_task = asyncio.create_task(http_server.serve())

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

    adapters: dict[str, LangGraphAdapter] = {}
    for entrypoint in _entrypoints():
        module_name, attribute = entrypoint.split(":", 1)
        target = getattr(importlib.import_module(module_name), attribute)
        adapter = LangGraphAdapter(
            target,
            persistence_mode="platform-managed",
            providers=postgres_persistence(persistence.checkpointer, persistence.store),
        )
        adapters[adapter.descriptor.graph_id] = adapter

    sessions = create_session_factory(engine)
    runs = PostgresRunRepository(sessions)
    threads = PostgresThreadRepository(sessions)
    application_id = os.environ.get("UR_APPLICATION_ID", "default")
    environment = os.environ.get("UR_KAFKA_ENVIRONMENT", "local")
    kafka_servers = os.environ.get("UR_KAFKA_BOOTSTRAP_SERVERS")
    queue = (
        AioKafkaRunCommandQueue(
            bootstrap_servers=kafka_servers,
            prefix=os.environ.get("UR_TOPIC_PREFIX", "rt"),
            environment=environment,
            application_id=application_id,
            group_id=os.environ.get(
                "UR_WORKER_CONSUMER_GROUP",
                f"rt.{environment}.{application_id}.workers.v1",
            ),
        )
        if kafka_servers
        else None
    )

    worker = WorkerServer.create(
        configured_concurrency=config.worker_max_concurrency,
        policy_ceiling=int(os.getenv("UR_WORKER_POLICY_CEILING", "64")),
        adapter=adapters,
        migrate_app=create_application_migration_handler(engine),
    )
    stop = asyncio.Event()
    active_tasks: set[asyncio.Task[None]] = set()

    async def execution_loop() -> None:
        if queue is None:
            return
        import grpc

        channel = grpc.aio.insecure_channel(os.environ["UR_GATEWAY_GRPC_TARGET"])
        try:
            while not stop.is_set():
                try:
                    receipt = await queue.receive(
                        WorkerId.parse(os.environ.get("UR_INSTANCE_ID", "all"))
                    )
                    await worker.worker.acquire()
                    task = asyncio.create_task(
                        process_receipt(
                            receipt,
                            worker.worker,
                            channel,
                            queue,
                            adapters,
                            runs,
                            threads,
                        )
                    )
                    active_tasks.add(task)
                    task.add_done_callback(active_tasks.discard)
                except asyncio.CancelledError:
                    break
                except RuntimeFailure as exc:
                    if exc.code is ErrorCode.QUEUE_CLOSED:
                        break
        finally:
            await channel.close()

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, stop.set)
    loop.add_signal_handler(signal.SIGINT, stop.set)

    await worker.start_listening(config.grpc_host, config.grpc_port)
    try:
        registration = await register_with_gateway(adapters, config)
        execution_task = asyncio.create_task(execution_loop())
        heartbeat_task = asyncio.create_task(
            heartbeat_gateway(
                worker,
                stop,
                interval_seconds=registration.heartbeat_interval_seconds,
            )
        )
        try:
            await stop.wait()
        finally:
            execution_task.cancel()
            heartbeat_task.cancel()
            await asyncio.gather(execution_task, heartbeat_task, return_exceptions=True)
            if active_tasks:
                await asyncio.gather(*active_tasks, return_exceptions=True)
    finally:
        await worker.stop(config.worker_drain_timeout_seconds)
        http_server.should_exit = True
        await http_task
        if queue is not None:
            await queue.close()
        await context.__aexit__(None, None, None)
        await engine.dispose()


def _entrypoints() -> list[str]:
    raw = os.environ.get("UR_APPLICATION_ENTRYPOINTS") or os.environ.get(
        "UR_APPLICATION_ENTRYPOINT"
    )
    if not raw:
        raise RuntimeError("UR_APPLICATION_ENTRYPOINT or UR_APPLICATION_ENTRYPOINTS is required")
    return [item.strip() for item in raw.split(",") if item.strip()]


def main(*, run_forever: bool = True) -> int:
    config = LauncherConfig.from_environment()
    config.validate()
    if run_forever:
        asyncio.run(_serve(config))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
