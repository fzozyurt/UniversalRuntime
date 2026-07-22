from __future__ import annotations

import asyncio
import importlib
import os
import signal
from collections.abc import Callable
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any

import httpx

from universal_runtime.adapters.grpc import WorkerServer
from universal_runtime.adapters.kafka import AioKafkaRunCommandQueue, TopicNames
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
    adapter: object | None = None, *, migrate_app: Callable[..., Any] | None = None
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
    entrypoints = _entrypoints()
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
    adapters = {}
    for entrypoint in entrypoints:
        module_name, attribute = entrypoint.split(":", 1)
        target = getattr(importlib.import_module(module_name), attribute)
        adapter = LangGraphAdapter(
            target,
            persistence_mode="platform-managed",
            providers=postgres_persistence(persistence.checkpointer, persistence.store),
        )
        adapters[adapter.descriptor.graph_id] = adapter

    async def _migrate_app(_request: Any) -> None:
        from sqlalchemy import text

        async with engine.connect() as _conn:
            await _conn.execute(text("SELECT 1"))
            await _conn.commit()

    session_factory = create_session_factory(engine)
    runs = PostgresRunRepository(session_factory)
    threads = PostgresThreadRepository(session_factory)
    instance_id = os.environ.get("UR_INSTANCE_ID", "worker")

    queue: AioKafkaRunCommandQueue | None = None
    kafka_servers = os.environ.get("UR_KAFKA_BOOTSTRAP_SERVERS")
    if kafka_servers:
        prefix = os.environ.get("UR_TOPIC_PREFIX", "rt")
        app_id = os.environ.get("UR_APPLICATION_ID", "default")
        queue = AioKafkaRunCommandQueue(
            bootstrap_servers=kafka_servers,
            prefix=prefix,
            application_id=app_id,
            group_id=f"{app_id}.worker",
        )

    server = create_server(adapters, migrate_app=_migrate_app)
    await server.start_listening(config.grpc_host, config.grpc_port)
    for adapter in adapters.values():
        await _register_with_gateway(adapter, config)

    max_conc = int(os.environ.get("UR_WORKER_MAX_CONCURRENCY", "8"))
    exec_sem = asyncio.Semaphore(max_conc)
    stop = asyncio.Event()

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
                    await exec_sem.acquire()
                    _ = asyncio.create_task(  # noqa: RUF006
                        _process_receipt(receipt, exec_sem, gateway_channel)
                    )
                except asyncio.CancelledError:
                    break
                except RuntimeFailure as exc:
                    if exc.code is ErrorCode.QUEUE_CLOSED:
                        break
        finally:
            if gateway_channel is not None:
                await gateway_channel.close()

    async def _process_receipt(receipt: Any, sem: asyncio.Semaphore, gateway_channel: Any) -> None:
        try:
            run = await runs.get(str(receipt.identity.run_id))
            if run.status is not RunStatus.PENDING:
                await queue.acknowledge(receipt)
                return
            await runs.update(run.mark_running(datetime.now(UTC)))
            adapter_obj = adapters.get(str(receipt.identity.assistant_id))
            if adapter_obj is None:
                await queue.acknowledge(receipt)
                return

            # Convert draft events to proto for gRPC streaming
            if gateway_channel is not None:
                await _stream_via_grpc(receipt, run, adapter_obj, gateway_channel)
            else:
                await _stream_local(receipt, run, adapter_obj)

            await queue.acknowledge(receipt)
        finally:
            sem.release()

    async def _stream_via_grpc(receipt: Any, run: Any, adapter_obj: Any, channel: Any) -> None:
        from google.protobuf import struct_pb2

        from universal_runtime.adapters.grpc.generated.runtime.v1 import (
            execution_pb2,
            worker_pb2_grpc,
        )
        from universal_runtime.adapters.grpc.payloads import python_to_value

        stub = worker_pb2_grpc.EventIngressServiceStub(channel)
        identity_proto = _identity_to_proto(run.identity)

        async def _gen() -> Any:
            seq = 0
            try:
                async for draft in adapter_obj.stream(receipt.command.request):
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
                    await _maybe_take_action(run, draft, runs, threads)
            except Exception as exc:
                await _fail_if_not_done(run, exc, runs)

        await stub.StreamEvents(_gen(), timeout=None)
        await _mark_idle_if_needed(run, runs, threads)

    async def _stream_local(receipt: Any, run: Any, adapter_obj: Any) -> None:
        try:
            async for draft in adapter_obj.stream(receipt.command.request):
                await _maybe_take_action(run, draft, runs, threads)
        except Exception as exc:
            await _fail_if_not_done(run, exc, runs)
        await _mark_idle_if_needed(run, runs, threads)

    async def _maybe_take_action(run: Any, draft: Any, rs: Any, ts: Any) -> None:
        if draft.type is RuntimeEventType.STATE_VALUES:
            return
        if draft.type not in TERMINAL_EVENT_TYPES:
            return
        current = await rs.get(str(run.run_id))
        if draft.type is RuntimeEventType.RUN_COMPLETED:
            await rs.update(current.complete(draft.data, datetime.now(UTC)))
        elif draft.type is RuntimeEventType.RUN_CANCELLED:
            await rs.update(current.cancel(datetime.now(UTC)))
        elif draft.type is RuntimeEventType.RUN_INTERRUPTED:
            await rs.update(
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
            await rs.update(
                current.fail(
                    RunError("FRAMEWORK_EXECUTION_FAILED", str(draft.data)),
                    datetime.now(UTC),
                )
            )
        if current.thread_id is not None:
            thread = await ts.get(str(current.thread_id))
            if draft.type is RuntimeEventType.RUN_INTERRUPTED:
                await ts.update(thread.mark_interrupted(datetime.now(UTC)))
            else:
                await ts.update(thread.mark_idle(datetime.now(UTC)))

    async def _fail_if_not_done(run: Any, exc: Exception, rs: Any) -> None:
        current = await rs.get(str(run.run_id))
        if current.status not in FINAL_STATUSES:
            await rs.update(current.fail(RunError("EXECUTION_FAILED", str(exc)), datetime.now(UTC)))

    async def _mark_idle_if_needed(run: Any, rs: Any, ts: Any) -> None:
        current = await rs.get(str(run.run_id))
        if current.thread_id is not None:
            thread = await ts.get(str(current.thread_id))
            await ts.update(thread.mark_idle(datetime.now(UTC)))

    async def _heartbeat_loop() -> None:
        from sqlalchemy import text

        _worker_id_str = instance_id
        _app_id = os.environ.get("UR_APPLICATION_ID", "default")
        while not stop.is_set():
            try:
                async with engine.begin() as conn:
                    await conn.execute(
                        text(
                            """INSERT INTO rt_exec.workers (id, worker_id, deployment_id, status, capabilities, updated_at)
                            VALUES (:id, :wid, :did, 'active', '{}', NOW())
                            ON CONFLICT (worker_id) DO UPDATE SET status = 'active', updated_at = NOW()"""
                        ),
                        {"id": _worker_id_str, "wid": _worker_id_str, "did": _app_id},
                    )
            except Exception:
                import logging

                logging.getLogger(__name__).exception("heartbeat update failed")
            await asyncio.sleep(15)

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, stop.set)
    loop.add_signal_handler(signal.SIGINT, stop.set)

    exec_task = asyncio.create_task(_execution_loop())
    hb_task = asyncio.create_task(_heartbeat_loop())
    try:
        await stop.wait()
    finally:
        exec_task.cancel()
        hb_task.cancel()
        await asyncio.gather(exec_task, hb_task, return_exceptions=True)
        await server.stop(config.worker_drain_timeout_seconds)
        await context.__aexit__(None, None, None)
        if queue is not None:
            await queue.close()
        await engine.dispose()


def _worker_id() -> Any:
    from universal_runtime.domain.identity import WorkerId

    return WorkerId.parse(os.environ.get("UR_INSTANCE_ID", "worker"))


async def _register_with_gateway(adapter: LangGraphAdapter, config: LauncherConfig) -> None:
    url = os.environ.get("UR_GATEWAY_REGISTER_URL")
    if not url:
        return
    instance_id = os.environ.get("UR_INSTANCE_ID", "worker")
    manifest = adapter.manifest
    prefix = os.environ.get("UR_TOPIC_PREFIX", "rt")
    app_id = os.environ.get("UR_APPLICATION_ID", "default")
    payload: dict[str, object] = {
        "worker_id": instance_id,
        "pod_name": os.environ.get("HOSTNAME", instance_id),
        "target": os.environ.get(
            "UR_WORKER_ADVERTISE_TARGET", f"{config.grpc_host}:{config.grpc_port}"
        ),
        "application_id": app_id,
        "workspace_key": os.environ.get("UR_WORKSPACE_KEY", "default"),
        "app_version": os.environ.get("ARTIFACT_VERSION", "unknown"),
        "alembic_version": os.environ.get("ARTIFACT_VERSION", "unknown"),
        "run_topic": os.environ.get("UR_RUN_TOPIC")
        or TopicNames.run_topic_for(prefix, app_id, 100),
        "graph_id": adapter.descriptor.graph_id,
        "manifest": {
            "adapter_id": manifest.adapter_id,
            "adapter_version": manifest.adapter_version,
            "profiles": list(manifest.supported_profiles),
            "capabilities": asdict(manifest.capabilities),
        },
    }
    timeout = httpx.Timeout(10.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        for attempt in range(10):
            try:
                response = await client.post(url, json=payload)
                response.raise_for_status()
                return
            except (httpx.HTTPError, OSError) as exc:
                if attempt >= 9:
                    raise RuntimeError(f"worker registration failed: {url}") from exc
                await asyncio.sleep(1)


def _entrypoints() -> list[str]:
    raw = os.environ.get("UR_APPLICATION_ENTRYPOINTS") or os.environ.get(
        "UR_APPLICATION_ENTRYPOINT"
    )
    if not raw:
        raise RuntimeError("UR_APPLICATION_ENTRYPOINT or UR_APPLICATION_ENTRYPOINTS is required")
    return [item.strip() for item in raw.split(",") if item.strip()]


__all__ = ["main"]
