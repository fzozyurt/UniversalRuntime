from __future__ import annotations

import asyncio
import importlib
import logging
import os
import signal
from dataclasses import asdict

import httpx

from universal_runtime.adapters.grpc import WorkerServer
from universal_runtime.adapters.langgraph import LangGraphAdapter
from universal_runtime.adapters.langgraph.persistence import postgres_persistence
from universal_runtime.adapters.postgres.database import create_engine
from universal_runtime.adapters.postgres.langgraph import managed_langgraph_persistence
from universal_runtime.bootstrap.runtime_config import LauncherConfig

_LOGGER = logging.getLogger(__name__)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def create_server(adapter: object | None = None) -> WorkerServer:
    config = LauncherConfig.from_environment()
    return WorkerServer.create(
        configured_concurrency=int(
            os.getenv("UR_WORKER_CONFIGURED_CONCURRENCY", str(config.worker_max_concurrency))
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
    database_url = os.environ.get("UR_STATE_DATABASE_URL") or os.environ["UR_DATABASE_URL"]
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
            providers=postgres_persistence(persistence.checkpointer, persistence.store),
        )
        graph_id = adapter.descriptor.graph_id
        if graph_id in adapters:
            raise RuntimeError(f"duplicate graph_id in application image: {graph_id}")
        adapters[graph_id] = adapter
        graph_entrypoints[graph_id] = entrypoint

    server = create_server(adapters)
    await server.start_listening(config.grpc_host, config.grpc_port)
    await _publish_registration(
        adapters,
        graph_entrypoints,
        server,
        config,
        attempts=10,
    )
    heartbeat_task = asyncio.create_task(
        _registration_loop(adapters, graph_entrypoints, server, config)
    )
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, stopped.set)
    loop.add_signal_handler(signal.SIGINT, stopped.set)
    try:
        await stopped.wait()
    finally:
        heartbeat_task.cancel()
        await asyncio.gather(heartbeat_task, return_exceptions=True)
        try:
            await _publish_registration(
                adapters,
                graph_entrypoints,
                server,
                config,
                status_override="draining",
                attempts=1,
            )
        except Exception:
            _LOGGER.exception("failed to publish worker draining status")
        await server.stop(config.worker_drain_timeout_seconds)
        await persistence_context.__aexit__(None, None, None)
        await engine.dispose()


async def _registration_loop(
    adapters: dict[str, LangGraphAdapter],
    graph_entrypoints: dict[str, str],
    server: WorkerServer,
    config: LauncherConfig,
) -> None:
    interval = max(1, int(os.environ.get("UR_WORKER_HEARTBEAT_SECONDS", "10")))
    while True:
        await asyncio.sleep(interval)
        try:
            await _publish_registration(
                adapters,
                graph_entrypoints,
                server,
                config,
                attempts=3,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOGGER.exception("worker heartbeat publication failed")


def _graph_descriptors(
    adapters: dict[str, LangGraphAdapter],
    graph_entrypoints: dict[str, str],
) -> list[dict[str, object]]:
    return [
        {
            "graph_id": graph_id,
            "entrypoint": graph_entrypoints[graph_id],
            "descriptor": {
                **asdict(adapter.descriptor),
                "entrypoint": graph_entrypoints[graph_id],
            },
        }
        for graph_id, adapter in sorted(adapters.items())
    ]


async def _publish_registration(
    adapters: dict[str, LangGraphAdapter],
    graph_entrypoints: dict[str, str],
    server: WorkerServer,
    config: LauncherConfig,
    *,
    status_override: str | None = None,
    attempts: int,
) -> None:
    """Advertise one immutable application revision and current replica capacity."""
    url = os.environ.get("UR_GATEWAY_REGISTER_URL")
    if not url:
        return
    manifests = {
        graph_id: {
            "adapter_id": adapter.manifest.adapter_id,
            "adapter_version": adapter.manifest.adapter_version,
            "profiles": sorted(adapter.manifest.supported_profiles),
            "capabilities": asdict(adapter.manifest.capabilities),
        }
        for graph_id, adapter in adapters.items()
    }
    target = os.environ.get(
        "UR_WORKER_ADVERTISE_TARGET", f"{config.grpc_host}:{config.grpc_port}"
    )
    status = status_override or (
        "busy" if server.worker.available_slots == 0 else "ready"
    )
    available_slots = 0 if status == "draining" else server.worker.available_slots
    revision_id = os.environ.get("UR_REVISION_ID", "active")
    payload = {
        "worker_id": os.environ.get("UR_INSTANCE_ID", "worker"),
        "target": target,
        "grpc_target": target,
        "workspace_id": os.environ.get("UR_WORKSPACE_ID", "default"),
        "project_id": os.environ.get("UR_PROJECT_ID", "default"),
        "application_id": os.environ.get("UR_APPLICATION_ID", "default"),
        "application_name": os.environ.get("UR_APPLICATION_NAME", "runtime-application"),
        "revision_id": revision_id,
        "deployment_id": os.environ.get("UR_DEPLOYMENT_ID", "local"),
        "environment": os.environ.get("UR_KAFKA_ENVIRONMENT", "local"),
        "image_digest": os.environ.get("UR_IMAGE_DIGEST", f"local:{revision_id}"),
        "activate_revision": _env_bool("UR_ACTIVATE_REVISION"),
        "graph_ids": sorted(adapters),
        "graphs": _graph_descriptors(adapters, graph_entrypoints),
        "manifests": manifests,
        "revision_metadata": {
            "source": os.environ.get("UR_SOURCE_REVISION", revision_id),
            "runtime_version": "0.1.0",
        },
        "max_concurrency": server.worker.max_concurrency,
        "active_executions": server.worker.active_executions,
        "available_slots": available_slots,
        "status": status,
    }
    timeout = httpx.Timeout(3.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        last_error: Exception | None = None
        for attempt in range(max(1, attempts)):
            try:
                response = await client.post(url, json=payload)
                response.raise_for_status()
                return
            except (httpx.HTTPError, OSError) as exc:
                last_error = exc
                if attempt + 1 < attempts:
                    await asyncio.sleep(1)
        if last_error is not None:
            raise RuntimeError(f"worker registration failed: {url}") from last_error


def _entrypoints() -> tuple[str, ...]:
    raw = os.environ.get("UR_APPLICATION_ENTRYPOINTS") or os.environ.get(
        "UR_APPLICATION_ENTRYPOINT"
    )
    if not raw:
        raise RuntimeError("UR_APPLICATION_ENTRYPOINT or UR_APPLICATION_ENTRYPOINTS is required")
    return tuple(item.strip() for item in raw.split(",") if item.strip())


__all__ = ["main"]
