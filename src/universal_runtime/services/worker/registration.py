from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from typing import Any

import grpc
from google.protobuf import struct_pb2, timestamp_pb2

from universal_runtime.adapters.grpc.generated.runtime.v1 import worker_pb2, worker_pb2_grpc
from universal_runtime.adapters.kafka import TopicNames
from universal_runtime.bootstrap.runtime_config import LauncherConfig

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class RegistrationResult:
    heartbeat_interval_seconds: int
    run_topic: str


async def register_with_gateway(
    adapters: dict[str, Any],
    config: LauncherConfig,
) -> RegistrationResult:
    target = os.environ.get("UR_GATEWAY_CONTROL_GRPC_TARGET", "").strip()
    environment = os.environ.get("UR_KAFKA_ENVIRONMENT", "local")
    application_id = os.environ.get("UR_APPLICATION_ID", "default")
    prefix = os.environ.get("UR_TOPIC_PREFIX", "rt")
    run_topic = os.environ.get("UR_RUN_TOPIC") or TopicNames.run_topic_for(
        prefix,
        application_id,
        100,
        environment=environment,
    )
    default_interval = int(os.environ.get("UR_WORKER_HEARTBEAT_SECONDS", "15"))
    if not target:
        return RegistrationResult(default_interval, run_topic)
    if not adapters:
        raise RuntimeError("worker cannot register without at least one runtime adapter")

    manifests = [adapter.manifest for adapter in adapters.values()]
    first = manifests[0]
    profiles = sorted(
        {profile for manifest in manifests for profile in manifest.supported_profiles}
    )
    stream_modes = sorted(
        {mode for manifest in manifests for mode in manifest.supported_stream_modes}
    )
    capabilities = worker_pb2.WorkerCapabilities(
        adapter_id=first.adapter_id,
        adapter_version=first.adapter_version,
        profiles=profiles,
        stream_modes=stream_modes,
        checkpoint=any(manifest.capabilities.checkpoint for manifest in manifests),
        state=any(manifest.capabilities.state_management for manifest in manifests),
        history=any(manifest.capabilities.history for manifest in manifests),
        interrupt=any(manifest.capabilities.interrupt for manifest in manifests),
        resume=any(manifest.capabilities.resume for manifest in manifests),
        custom_http=any(manifest.capabilities.custom_http for manifest in manifests),
        a2a=any(manifest.capabilities.a2a for manifest in manifests),
        session_affinity=str(first.session_affinity),
    )
    metadata = struct_pb2.Struct()
    metadata.update(
        {
            "target": os.environ.get(
                "UR_WORKER_ADVERTISE_TARGET",
                f"{config.grpc_host}:{config.grpc_port}",
            ),
            "workspace_key": os.environ.get("UR_WORKSPACE_KEY", "default"),
            "app_version": os.environ.get("ARTIFACT_VERSION", "development"),
            "run_topic": run_topic,
            "migration_enabled": bool(os.environ.get("UR_APPLICATION_MIGRATIONS_PATH", "").strip()),
            "migration_revision": os.environ.get(
                "UR_APPLICATION_MIGRATION_REVISION",
                "head",
            ),
            "graphs": sorted(adapters),
        }
    )
    request = worker_pb2.RegisterWorkerRequest(
        worker_id=os.environ.get("UR_INSTANCE_ID", "worker"),
        application_id=application_id,
        revision_id=os.environ.get(
            "UR_REVISION_ID",
            os.environ.get("ARTIFACT_VERSION", "development"),
        ),
        deployment_id=os.environ.get("UR_DEPLOYMENT_ID", application_id),
        pod_name=os.environ.get("HOSTNAME", os.environ.get("UR_INSTANCE_ID", "worker")),
        max_concurrency=int(
            os.environ.get("UR_WORKER_MAX_CONCURRENCY", str(config.worker_max_concurrency))
        ),
        config_hash=os.environ.get("UR_CONFIG_HASH", ""),
        capabilities=capabilities,
        metadata=metadata,
    )

    timeout = float(os.environ.get("UR_WORKER_REGISTRATION_TIMEOUT_SECONDS", "360"))
    attempts = int(os.environ.get("UR_WORKER_REGISTRATION_ATTEMPTS", "10"))
    last_error: Exception | None = None
    for attempt in range(attempts):
        channel = grpc.aio.insecure_channel(target)
        try:
            stub = worker_pb2_grpc.WorkerControlServiceStub(channel)
            response = await stub.Register(request, timeout=timeout)
            if not response.accepted:
                raise RuntimeError(response.reason or "worker registration rejected")
            resolved = dict(response.resolved_execution_defaults)
            return RegistrationResult(
                heartbeat_interval_seconds=response.heartbeat_interval_seconds or default_interval,
                run_topic=str(resolved.get("run_topic") or run_topic),
            )
        except RuntimeError:
            raise
        except Exception as exc:
            last_error = exc
            if attempt + 1 >= attempts:
                break
            await asyncio.sleep(min(5.0, 0.5 * (attempt + 1)))
        finally:
            await channel.close()
    raise RuntimeError(f"worker registration failed: {target}") from last_error


async def heartbeat_gateway(
    server: Any,
    stop: asyncio.Event,
    *,
    interval_seconds: int,
) -> None:
    target = os.environ.get("UR_GATEWAY_CONTROL_GRPC_TARGET", "").strip()
    if not target:
        return
    worker_id = os.environ.get("UR_INSTANCE_ID", "worker")

    while not stop.is_set():
        channel = grpc.aio.insecure_channel(target)
        try:
            stub = worker_pb2_grpc.WorkerControlServiceStub(channel)

            async def messages() -> Any:
                while not stop.is_set():
                    timestamp = timestamp_pb2.Timestamp()
                    timestamp.GetCurrentTime()
                    yield worker_pb2.WorkerMessage(
                        heartbeat=worker_pb2.WorkerHeartbeat(
                            worker_id=worker_id,
                            timestamp=timestamp,
                            active_executions=server.worker.active_executions,
                            available_slots=server.worker.available_slots,
                        )
                    )
                    try:
                        await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
                    except TimeoutError:
                        pass

            async for _response in stub.Work(messages()):
                if stop.is_set():
                    break
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOGGER.exception("worker heartbeat stream disconnected")
            try:
                await asyncio.wait_for(stop.wait(), timeout=min(5, interval_seconds))
            except TimeoutError:
                pass
        finally:
            await channel.close()
