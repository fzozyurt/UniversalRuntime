from __future__ import annotations

import asyncio
import importlib
import logging
import os
import signal
from collections.abc import Callable
from typing import Any

from universal_runtime.adapters.grpc import WorkerServer
from universal_runtime.adapters.kafka import (
    AioKafkaRunCommandQueue,
    AioKafkaRuntimeEventPublisher,
)
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
from universal_runtime.services.worker.execution import process_receipt
from universal_runtime.services.worker.migrations import create_application_migration_handler
from universal_runtime.services.worker.registration import heartbeat_gateway, register_with_gateway

_LOGGER = logging.getLogger(__name__)


def create_server(
    adapter: object | None = None,
    *,
    migrate_app: Callable[..., Any] | None = None,
) -> WorkerServer:
    config = LauncherConfig.from_environment()
    return WorkerServer.create(
        configured_concurrency=int(
            os.getenv("UR_WORKER_CONFIGURED_CONCURRENCY", str(config.worker_max_concurrency))
        ),
        policy_ceiling=int(os.getenv("UR_WORKER_POLICY_CEILING", "64")),
        adapter=adapter,
        migrate_app=migrate_app,
    )


def main(*, run_forever: bool = True) -> int:
    config = LauncherConfig.from_environment()
    config.validate()
    if run_forever:
        asyncio.run(_serve(config))
    return 0


async def _serve(config: LauncherConfig) -> None:
    from universal_runtime.telemetry import init_observability

    init_observability()
    database_url = os.environ["UR_DATABASE_URL"]
    engine = create_engine(
        database_url,
        pool_size=int(os.environ.get("UR_WORKER_DB_POOL_SIZE", "30")),
        max_overflow=int(os.environ.get("UR_WORKER_DB_MAX_OVERFLOW", "10")),
    )
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

    session_factory = create_session_factory(engine)
    runs = PostgresRunRepository(session_factory)
    threads = PostgresThreadRepository(session_factory)
    application_id = os.environ.get("UR_APPLICATION_ID", "default")
    environment = os.environ.get("UR_KAFKA_ENVIRONMENT", "local")
    prefix = os.environ.get("UR_TOPIC_PREFIX", "rt")

    queue: AioKafkaRunCommandQueue | None = None
    event_publisher: AioKafkaRuntimeEventPublisher | None = None
    kafka_servers = os.environ.get("UR_KAFKA_BOOTSTRAP_SERVERS")
    if kafka_servers:
        queue = AioKafkaRunCommandQueue(
            bootstrap_servers=kafka_servers,
            prefix=prefix,
            environment=environment,
            application_id=application_id,
            group_id=os.environ.get(
                "UR_WORKER_CONSUMER_GROUP",
                f"rt.{environment}.{application_id}.workers.v1",
            ),
        )
        event_publisher = AioKafkaRuntimeEventPublisher(
            bootstrap_servers=kafka_servers,
            prefix=prefix,
            environment=environment,
            application_id=application_id,
        )

    server = create_server(
        adapters,
        migrate_app=create_application_migration_handler(engine),
    )
    stop = asyncio.Event()
    active_tasks: set[asyncio.Task[None]] = set()

    async def execution_loop() -> None:
        if queue is None:
            return
        while not stop.is_set():
            try:
                receipt = await queue.receive(_worker_id())
                await server.worker.acquire()
                task = asyncio.create_task(
                    process_receipt(
                        receipt,
                        server.worker,
                        event_publisher,
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
                _LOGGER.exception("worker queue receive failed")

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, stop.set)
    loop.add_signal_handler(signal.SIGINT, stop.set)

    await server.start_listening(config.grpc_host, config.grpc_port)
    try:
        registration = await register_with_gateway(adapters, config)
        execution_task = asyncio.create_task(execution_loop())
        heartbeat_task = asyncio.create_task(
            heartbeat_gateway(
                server,
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
        await server.stop(config.worker_drain_timeout_seconds)
        await context.__aexit__(None, None, None)
        if queue is not None:
            await queue.close()
        if event_publisher is not None:
            await event_publisher.close()
        await engine.dispose()


def _worker_id() -> Any:
    from universal_runtime.domain.identity import WorkerId

    return WorkerId.parse(os.environ.get("UR_INSTANCE_ID", "worker"))


def _entrypoints() -> list[str]:
    raw = os.environ.get("UR_APPLICATION_ENTRYPOINTS") or os.environ.get(
        "UR_APPLICATION_ENTRYPOINT"
    )
    if not raw:
        raise RuntimeError("UR_APPLICATION_ENTRYPOINT or UR_APPLICATION_ENTRYPOINTS is required")
    return [item.strip() for item in raw.split(",") if item.strip()]


__all__ = ["main"]
