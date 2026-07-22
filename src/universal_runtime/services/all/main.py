from __future__ import annotations

import asyncio
import importlib
import os
import signal
from datetime import UTC, datetime
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
from universal_runtime.domain.events import RuntimeEventType
from universal_runtime.domain.execution import RunError, RunStatus
from universal_runtime.domain.identity import WorkerId
from universal_runtime.services.gateway.app import create_app

TERMINAL_EVENT_TYPES = {
    RuntimeEventType.RUN_COMPLETED,
    RuntimeEventType.RUN_FAILED,
    RuntimeEventType.RUN_CANCELLED,
    RuntimeEventType.RUN_INTERRUPTED,
}

FINAL_STATUSES = {
    RunStatus.SUCCESS,
    RunStatus.ERROR,
    RunStatus.TIMEOUT,
    RunStatus.CANCELLED,
    RunStatus.INTERRUPTED,
}


async def _serve(config: LauncherConfig) -> None:
    """Run Gateway and Worker in one process."""
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

    max_conc = int(os.environ.get("UR_WORKER_MAX_CONCURRENCY", "8"))
    exec_sem = asyncio.Semaphore(max_conc)

    async def _execution_loop() -> None:
        if queue is None:
            return
        import grpc

        gw_target = "127.0.0.1:" + os.environ.get("UR_GATEWAY_GRPC_PORT", "9091")
        ch = grpc.aio.insecure_channel(gw_target)
        try:
            while not stop.is_set():
                try:
                    receipt = await queue.receive(
                        WorkerId.parse(os.environ.get("UR_INSTANCE_ID", "all"))
                    )
                    await exec_sem.acquire()
                    _ = asyncio.create_task(  # noqa: RUF006
                        _process_receipt(receipt, exec_sem, ch, adapter, runs, threads)
                    )
                except asyncio.CancelledError:
                    break
                except RuntimeFailure as exc:
                    if exc.code is ErrorCode.QUEUE_CLOSED:
                        break
        finally:
            await ch.close()

    async def _process_receipt(
        receipt: Any,
        sem: asyncio.Semaphore,
        ch: Any,
        adapter_obj: Any,
        rs: Any,
        ts: Any,
    ) -> None:
        try:
            run = await rs.get(str(receipt.identity.run_id))
            if run.status is not RunStatus.PENDING:
                await queue.acknowledge(receipt)
                return
            await rs.update(run.mark_running(datetime.now(UTC)))
            await _send_events_to_gateway(
                ch, adapter_obj, receipt.identity, receipt.command.request, rs, ts
            )
            await queue.acknowledge(receipt)
        finally:
            sem.release()

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


async def _send_events_to_gateway(
    channel: Any, adapter_obj: Any, identity: Any, request: Any, runs: Any, threads: Any
) -> None:
    from google.protobuf import struct_pb2

    from universal_runtime.adapters.grpc.generated.runtime.v1 import execution_pb2, worker_pb2_grpc
    from universal_runtime.adapters.grpc.payloads import python_to_value

    stub = worker_pb2_grpc.EventIngressServiceStub(channel)
    identity_proto = execution_pb2.ExecutionIdentity(
        workspace_id=str(identity.scope.workspace_id),
        project_id=str(identity.scope.project_id),
        application_id=str(identity.scope.application_id),
        revision_id=str(identity.scope.revision_id),
        deployment_id=str(identity.scope.deployment_id),
        assistant_id=str(identity.assistant_id),
        thread_id=str(identity.thread_id) if identity.thread_id else "",
        run_id=str(identity.run_id),
        attempt_id=str(identity.attempt_id),
    )

    async def _event_stream() -> Any:
        seq = 0
        last_result: Any = None
        terminal = False
        try:
            async for draft in adapter_obj.stream(request):
                pe = execution_pb2.RuntimeEvent(
                    type=str(draft.type),
                    sequence=seq,
                    namespace=list(draft.namespace),
                    data=python_to_value(draft.data),
                    native=struct_pb2.Struct(
                        fields={k: python_to_value(v) for k, v in draft.native.items()}
                    ),
                )
                pe.identity.CopyFrom(identity_proto)
                pe.timestamp.FromDatetime(datetime.now(UTC))
                yield pe
                seq += 1
                if draft.type is RuntimeEventType.STATE_VALUES:
                    last_result = draft.data
                if draft.type in TERMINAL_EVENT_TYPES:
                    terminal = True
                    current = await runs.get(str(identity.run_id))
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
                current = await runs.get(str(identity.run_id))
                await runs.update(current.complete(last_result, datetime.now(UTC)))
            run_obj = await runs.get(str(identity.run_id))
            if run_obj.thread_id is not None:
                thread = await threads.get(str(run_obj.thread_id))
                await threads.update(thread.mark_idle(datetime.now(UTC)))
        except Exception as exc:
            current = await runs.get(str(identity.run_id))
            if current.status not in FINAL_STATUSES:
                await runs.update(
                    current.fail(RunError("EXECUTION_FAILED", str(exc)), datetime.now(UTC))
                )

    await stub.StreamEvents(_event_stream(), timeout=None)


def main(*, run_forever: bool = True) -> int:
    config = LauncherConfig.from_environment()
    config.validate()
    if run_forever:
        asyncio.run(_serve(config))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
