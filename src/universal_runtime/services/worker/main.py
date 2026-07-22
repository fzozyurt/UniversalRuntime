from __future__ import annotations

import asyncio
import importlib
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
    server = create_server(adapters)
    await server.start_listening(config.grpc_host, config.grpc_port)
    for adapter in adapters.values():
        await _register_with_gateway(adapter, config)
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, stopped.set)
    loop.add_signal_handler(signal.SIGINT, stopped.set)
    await stopped.wait()
    await server.stop(config.worker_drain_timeout_seconds)
    await context.__aexit__(None, None, None)
    await engine.dispose()


async def _register_with_gateway(adapter: LangGraphAdapter, config: LauncherConfig) -> None:
    """Advertise the worker and graph capability to the Gateway when configured."""
    url = os.environ.get("UR_GATEWAY_REGISTER_URL")
    if not url:
        return
    manifest = adapter.manifest
    payload = {
        "worker_id": os.environ.get("UR_INSTANCE_ID", "worker"),
        "target": os.environ.get("UR_WORKER_ADVERTISE_TARGET", f"{config.grpc_host}:{config.grpc_port}"),
        "application_id": os.environ.get("UR_APPLICATION_ID", "default"),
        "graph_id": adapter.descriptor.graph_id,
        "manifest": {
            "adapter_id": manifest.adapter_id,
            "adapter_version": manifest.adapter_version,
            "profiles": list(manifest.supported_profiles),
            "capabilities": asdict(manifest.capabilities),
        },
    }
    timeout = httpx.Timeout(3.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        last_error: Exception | None = None
        for _ in range(10):
            try:
                response = await client.post(url, json=payload)
                response.raise_for_status()
                return
            except (httpx.HTTPError, OSError) as exc:
                last_error = exc
                await asyncio.sleep(1)
        if last_error is not None:
            raise RuntimeError(f"worker registration failed: {url}") from last_error


def _entrypoints() -> list[str]:
    raw = os.environ.get("UR_APPLICATION_ENTRYPOINTS") or os.environ.get("UR_APPLICATION_ENTRYPOINT")
    if not raw:
        raise RuntimeError("UR_APPLICATION_ENTRYPOINT or UR_APPLICATION_ENTRYPOINTS is required")
    return [item.strip() for item in raw.split(",") if item.strip()]


__all__ = ["main"]
