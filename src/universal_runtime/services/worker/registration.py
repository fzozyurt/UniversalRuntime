from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import asdict, dataclass

import httpx

from universal_runtime.adapters.grpc import WorkerServer
from universal_runtime.adapters.langgraph import LangGraphAdapter
from universal_runtime.bootstrap.runtime_config import LauncherConfig

_LOGGER = logging.getLogger(__name__)
_TRUE_VALUES = {"1", "true", "yes", "on"}


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in _TRUE_VALUES


@dataclass(slots=True)
class WorkerRegistrationPublisher:
    adapters: dict[str, LangGraphAdapter]
    graph_entrypoints: dict[str, str]
    server: WorkerServer
    config: LauncherConfig

    def _graph_descriptors(self) -> list[dict[str, object]]:
        return [
            {
                "graph_id": graph_id,
                "entrypoint": self.graph_entrypoints[graph_id],
                "descriptor": {
                    **asdict(adapter.descriptor),
                    "entrypoint": self.graph_entrypoints[graph_id],
                },
            }
            for graph_id, adapter in sorted(self.adapters.items())
        ]

    def _payload(self, status_override: str | None = None) -> dict[str, object]:
        manifests = {
            graph_id: {
                "adapter_id": adapter.manifest.adapter_id,
                "adapter_version": adapter.manifest.adapter_version,
                "profiles": sorted(adapter.manifest.supported_profiles),
                "capabilities": asdict(adapter.manifest.capabilities),
            }
            for graph_id, adapter in self.adapters.items()
        }
        target = os.environ.get(
            "UR_WORKER_ADVERTISE_TARGET",
            f"{self.config.grpc_host}:{self.config.grpc_port}",
        )
        status = status_override or ("busy" if self.server.worker.available_slots == 0 else "ready")
        available_slots = 0 if status == "draining" else self.server.worker.available_slots
        revision_id = os.environ.get("UR_REVISION_ID", "active")
        return {
            "worker_id": os.environ.get("UR_INSTANCE_ID", "worker"),
            "target": target,
            "grpc_target": target,
            "workspace_id": os.environ.get("UR_WORKSPACE_ID", "default"),
            "project_id": os.environ.get("UR_PROJECT_ID", "default"),
            "application_id": os.environ.get("UR_APPLICATION_ID", "default"),
            "application_name": os.environ.get(
                "UR_APPLICATION_NAME",
                "runtime-application",
            ),
            "revision_id": revision_id,
            "deployment_id": os.environ.get("UR_DEPLOYMENT_ID", "local"),
            "environment": os.environ.get("UR_KAFKA_ENVIRONMENT", "local"),
            "image_digest": os.environ.get(
                "UR_IMAGE_DIGEST",
                f"local:{revision_id}",
            ),
            "activate_revision": _env_bool("UR_ACTIVATE_REVISION"),
            "graph_ids": sorted(self.adapters),
            "graphs": self._graph_descriptors(),
            "manifests": manifests,
            "revision_metadata": {
                "source": os.environ.get("UR_SOURCE_REVISION", revision_id),
                "runtime_version": "0.1.0",
            },
            "max_concurrency": self.server.worker.max_concurrency,
            "active_executions": self.server.worker.active_executions,
            "available_slots": available_slots,
            "status": status,
        }

    async def publish(
        self,
        *,
        status_override: str | None = None,
        attempts: int = 3,
    ) -> None:
        url = os.environ.get("UR_GATEWAY_REGISTER_URL")
        if not url:
            return
        timeout = httpx.Timeout(3.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            last_error: Exception | None = None
            for attempt in range(max(1, attempts)):
                try:
                    response = await client.post(
                        url,
                        json=self._payload(status_override),
                    )
                    response.raise_for_status()
                    return
                except (httpx.HTTPError, OSError) as exc:
                    last_error = exc
                    if attempt + 1 < attempts:
                        await asyncio.sleep(1)
            if last_error is not None:
                raise RuntimeError(f"worker registration failed: {url}") from last_error

    async def heartbeat_loop(self) -> None:
        interval = max(
            1,
            int(os.environ.get("UR_WORKER_HEARTBEAT_SECONDS", "10")),
        )
        while True:
            await asyncio.sleep(interval)
            try:
                await self.publish(attempts=3)
            except asyncio.CancelledError:
                raise
            except Exception:
                _LOGGER.exception("worker heartbeat publication failed")

    async def publish_draining(self) -> None:
        try:
            await self.publish(status_override="draining", attempts=1)
        except Exception:
            _LOGGER.exception("failed to publish worker draining status")
