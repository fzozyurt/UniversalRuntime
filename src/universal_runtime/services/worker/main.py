from __future__ import annotations

import asyncio
import importlib
import logging
import os
import signal
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

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
from universal_runtime.services.worker.migrations import create_application_migration_handler
from universal_runtime.services.worker.registration import heartbeat_gateway, register_with_gateway

_LOGGER = logging.getLogger(__name__)

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


def _identity_to_proto(identity: Any) -> Any:
    from universal_runtime.adapters.grpc.generated.runtime.v1 import execution_pb2

    return execution_pb2.ExecutionIdentity(
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

    queue: AioKafkaRunCommandQueue | None = None
    kafka_servers = os.environ.get("UR_KAFKA_BOOTSTRAP_SERVERS")
    if kafka_servers:
        queue = AioKafkaRunCommandQueue(
            bootstrap_servers=kafka_servers,
            prefix=os.environ.get("UR_TOPIC_PREFIX", "rt"),
            environment=environment,
            application_id=application_id,
            group_id=os.environ.get(
                "UR_WORKER_CONSUMER_GROUP",
                f"rt.{environment}.{application_id}.workers.v1",
            ),
        )

    server = create_server(
        adapters,
        migrate_app=create_application_migration_handler(engine),
    )
    stop = asyncio.Event()
    active_tasks: set[asyncio.Task[None]] = set()

    async def _execution_loop() -> None:
        if queue is None:
            return

        gateway_target = os.environ.get("UR_GATEWAY_GRPC_TARGET")
        gateway_channel = None
        if gateway_target:
            import grpc

            gateway_channel = grpc.aio.insecure_channel(gateway_target)

        try:
            while not stop.is_set():
                try:
                    receipt = await queue.receive(_worker_id())
                    await server.worker.acquire()
                    task = asyncio.create_task(
                        _process_receipt(
                            receipt,
                            server.worker,
                            gateway_channel,
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
        finally:
            if gateway_channel is not None:
                await gateway_channel.close()

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, stop.set)
    loop.add_signal_handler(signal.SIGINT, stop.set)

    await server.start_listening(config.grpc_host, config.grpc_port)
    try:
        registration = await register_with_gateway(adapters, config)
        execution_task = asyncio.create_task(_execution_loop())
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
        await engine.dispose()


async def _process_receipt(
    receipt: Any,
    bounded_worker: Any,
    gateway_channel: Any,
    queue: AioKafkaRunCommandQueue,
    adapters: dict[str, LangGraphAdapter],
    runs: Any,
    threads: Any,
) -> None:
    try:
        run = await runs.get(str(receipt.identity.run_id))
        if run.status is not RunStatus.PENDING:
            await queue.acknowledge(receipt)
            return

        adapter_obj = adapters.get(str(receipt.identity.assistant_id))
        if adapter_obj is None:
            await runs.update(
                run.fail(
                    RunError(
                        "ADAPTER_NOT_FOUND",
                        f"no adapter registered for assistant {receipt.identity.assistant_id}",
                    ),
                    datetime.now(UTC),
                )
            )
            await queue.acknowledge(receipt)
            return

        await runs.update(run.mark_running(datetime.now(UTC)))
        if gateway_channel is not None:
            await _stream_via_grpc(receipt, run, adapter_obj, gateway_channel, runs, threads)
        else:
            await _stream_local(receipt, run, adapter_obj, runs, threads)
        await queue.acknowledge(receipt)
    except asyncio.CancelledError:
        await queue.reject(receipt, retryable=True)
        raise
    except Exception:
        _LOGGER.exception("worker execution failed run_id=%s", receipt.identity.run_id)
        await queue.reject(receipt, retryable=True)
    finally:
        bounded_worker.release()


async def _stream_via_grpc(
    receipt: Any,
    run: Any,
    adapter_obj: Any,
    channel: Any,
    runs: Any,
    threads: Any,
) -> None:
    from google.protobuf import struct_pb2

    from universal_runtime.adapters.grpc.generated.runtime.v1 import (
        execution_pb2,
        worker_pb2_grpc,
    )
    from universal_runtime.adapters.grpc.payloads import python_to_value

    stub = worker_pb2_grpc.EventIngressServiceStub(channel)
    identity_proto = _identity_to_proto(run.identity)

    async def _events() -> Any:
        sequence = 0
        try:
            async for draft in adapter_obj.stream(receipt.command.request):
                event = execution_pb2.RuntimeEvent(
                    type=str(draft.type),
                    sequence=sequence,
                    namespace=list(draft.namespace),
                    data=python_to_value(draft.data),
                    native=struct_pb2.Struct(
                        fields={key: python_to_value(value) for key, value in draft.native.items()}
                    ),
                )
                event.identity.CopyFrom(identity_proto)
                event.timestamp.FromDatetime(datetime.now(UTC))
                yield event
                sequence += 1
                await _maybe_take_action(run, draft, runs, threads)
        except Exception as exc:
            await _fail_if_not_done(run, exc, runs)
            raise

    await stub.StreamEvents(_events(), timeout=None)
    await _mark_idle_if_needed(run, runs, threads)


async def _stream_local(
    receipt: Any,
    run: Any,
    adapter_obj: Any,
    runs: Any,
    threads: Any,
) -> None:
    try:
        async for draft in adapter_obj.stream(receipt.command.request):
            await _maybe_take_action(run, draft, runs, threads)
    except Exception as exc:
        await _fail_if_not_done(run, exc, runs)
        raise
    await _mark_idle_if_needed(run, runs, threads)


async def _maybe_take_action(run: Any, draft: Any, runs: Any, threads: Any) -> None:
    if draft.type not in TERMINAL_EVENT_TYPES:
        return
    current = await runs.get(str(run.run_id))
    if draft.type is RuntimeEventType.RUN_COMPLETED:
        await runs.update(current.complete(draft.data, datetime.now(UTC)))
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
        await threads.update(
            thread.mark_interrupted(datetime.now(UTC))
            if draft.type is RuntimeEventType.RUN_INTERRUPTED
            else thread.mark_idle(datetime.now(UTC))
        )


async def _fail_if_not_done(run: Any, exc: Exception, runs: Any) -> None:
    current = await runs.get(str(run.run_id))
    if current.status not in FINAL_STATUSES:
        await runs.update(
            current.fail(
                RunError("EXECUTION_FAILED", str(exc)),
                datetime.now(UTC),
            )
        )


async def _mark_idle_if_needed(run: Any, runs: Any, threads: Any) -> None:
    current = await runs.get(str(run.run_id))
    if current.status is RunStatus.INTERRUPTED or current.thread_id is None:
        return
    thread = await threads.get(str(current.thread_id))
    if thread.status.value == "busy":
        await threads.update(thread.mark_idle(datetime.now(UTC)))


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
