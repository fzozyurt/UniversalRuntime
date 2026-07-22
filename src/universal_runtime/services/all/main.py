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
from universal_runtime.adapters.postgres.events import PostgresEventJournal
from universal_runtime.adapters.postgres.langgraph import managed_langgraph_persistence
from universal_runtime.adapters.postgres.repositories import (
    PostgresRunRepository,
    PostgresThreadRepository,
)
from universal_runtime.bootstrap.runtime_config import LauncherConfig
from universal_runtime.domain.execution import RunStatus
from universal_runtime.services.gateway.app import create_app


async def _serve(config: LauncherConfig) -> None:
    """Run Gateway and Worker in one process.

    This is a compact deployment profile, not a local/in-memory shortcut.  It
    retains Kafka, PostgreSQL and the gRPC worker boundary, and can be scaled by
    running multiple identical ``all`` pods with distinct instance IDs.
    """
    from universal_runtime.telemetry import init_observability

    init_observability()

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

    session_factory = create_session_factory(engine)
    runs = PostgresRunRepository(session_factory)
    threads = PostgresThreadRepository(session_factory)
    events = PostgresEventJournal(session_factory)
    app_id = os.environ.get("UR_APPLICATION_ID", "default")
    kafka_servers = os.environ.get("UR_KAFKA_BOOTSTRAP_SERVERS")
    queue: AioKafkaRunCommandQueue | None = None
    if kafka_servers:
        queue = AioKafkaRunCommandQueue(
            bootstrap_servers=kafka_servers,
            prefix=os.environ.get("UR_TOPIC_PREFIX", "rt"),
            application_id=app_id,
            group_id=f"{app_id}.worker",
        )

    worker = WorkerServer.create(
        configured_concurrency=config.worker_max_concurrency,
        policy_ceiling=int(os.getenv("UR_WORKER_POLICY_CEILING", "64")),
        adapter=adapter,
        migrate_app=_migrate_app,
    )

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, stop.set)
    loop.add_signal_handler(signal.SIGINT, stop.set)

    async def _execution_loop() -> None:
        _queue = queue
        if _queue is None:
            return
        from datetime import UTC, datetime

        from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
        from universal_runtime.domain.events import RuntimeEventDraft, RuntimeEventType
        from universal_runtime.domain.execution import RunError
        from universal_runtime.domain.identity import WorkerId

        while not stop.is_set():
            try:
                receipt = await _queue.receive(
                    WorkerId.parse(os.environ.get("UR_INSTANCE_ID", "all"))
                )
                run = await runs.get(str(receipt.identity.run_id))
                if run.status is not RunStatus.PENDING:
                    await _queue.acknowledge(receipt)
                    continue
                await runs.update(run.mark_running(datetime.now(UTC)))
                terminal = False
                last_result: Any = None
                try:
                    async for draft in adapter.stream(receipt.command.request):
                        await events.append(draft)
                        if draft.type is RuntimeEventType.STATE_VALUES:
                            last_result = draft.data
                        if draft.type in {
                            RuntimeEventType.RUN_COMPLETED,
                            RuntimeEventType.RUN_FAILED,
                            RuntimeEventType.RUN_CANCELLED,
                            RuntimeEventType.RUN_INTERRUPTED,
                        }:
                            terminal = True
                            current = await runs.get(str(run.run_id))
                            if draft.type is RuntimeEventType.RUN_COMPLETED:
                                await runs.update(current.complete(last_result, datetime.now(UTC)))
                            elif draft.type is RuntimeEventType.RUN_CANCELLED:
                                await runs.update(current.cancel(datetime.now(UTC)))
                            elif draft.type is RuntimeEventType.RUN_INTERRUPTED:
                                await runs.update(
                                    type(current)(
                                        current.identity,
                                        RunStatus.INTERRUPTED,
                                        current.metadata,
                                        current.created_at,
                                        datetime.now(UTC),
                                        current.result,
                                        current.error,
                                    )
                                )
                            else:
                                await runs.update(
                                    current.fail(
                                        RunError("FRAMEWORK_EXECUTION_FAILED", str(draft.data)),
                                        datetime.now(UTC),
                                    )
                                )
                            if current.thread_id is not None:
                                thread = await threads.get(str(current.thread_id))
                                if draft.type is RuntimeEventType.RUN_INTERRUPTED:
                                    await threads.update(thread.mark_interrupted(datetime.now(UTC)))
                                else:
                                    await threads.update(thread.mark_idle(datetime.now(UTC)))
                    if not terminal:
                        current = await runs.get(str(run.run_id))
                        await runs.update(current.complete(last_result, datetime.now(UTC)))
                except Exception as exc:
                    current = await runs.get(str(run.run_id))
                    await events.append(
                        RuntimeEventDraft(
                            receipt.identity,
                            RuntimeEventType.RUN_FAILED,
                            data={"error": str(exc)},
                        )
                    )
                    if current.status not in {
                        RunStatus.SUCCESS,
                        RunStatus.ERROR,
                        RunStatus.TIMEOUT,
                        RunStatus.CANCELLED,
                        RunStatus.INTERRUPTED,
                    }:
                        await runs.update(
                            current.fail(RunError("EXECUTION_FAILED", str(exc)), datetime.now(UTC))
                        )
                await _queue.acknowledge(receipt)
            except asyncio.CancelledError:
                break
            except RuntimeFailure as exc:
                if exc.code is ErrorCode.QUEUE_CLOSED:
                    break

    try:
        await worker.start_listening("127.0.0.1", config.grpc_port)
        exec_task = asyncio.create_task(_execution_loop())
        await stop.wait()
        exec_task.cancel()
        await asyncio.gather(exec_task, return_exceptions=True)
        await worker.stop(config.worker_drain_timeout_seconds)
        http_server.should_exit = True
        await http_task
    finally:
        if queue is not None:
            await queue.close()
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
