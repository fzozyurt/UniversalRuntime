from __future__ import annotations

import json
import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import grpc
from fastapi import FastAPI
from google.protobuf import json_format, struct_pb2
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from universal_runtime.adapters.grpc.generated.runtime.v1 import worker_pb2, worker_pb2_grpc
from universal_runtime.adapters.postgres.database import create_engine
from universal_runtime.application.migration_coordination import ApplicationMigrationCoordinator


class GatewayWorkerControlServicer(worker_pb2_grpc.WorkerControlServiceServicer):
    """Gateway-side registration, migration election and heartbeat service."""

    def __init__(
        self,
        engine: AsyncEngine | None,
        *,
        environment: str,
        migration_timeout_seconds: float = 300,
    ) -> None:
        self._engine = engine
        self._environment = environment
        self._migration_timeout_seconds = migration_timeout_seconds
        self._coordinator = (
            ApplicationMigrationCoordinator(
                engine,
                claim_timeout_seconds=max(1, int(migration_timeout_seconds)),
            )
            if engine is not None
            else None
        )
        self._local_workers: dict[str, dict[str, Any]] = {}

    async def Register(  # noqa: N802
        self,
        request: worker_pb2.RegisterWorkerRequest,
        context: grpc.aio.ServicerContext,
    ) -> worker_pb2.RegisterWorkerResponse:
        del context
        metadata = _message_dict(request.metadata)
        target = str(metadata.get("target", ""))
        workspace_key = str(metadata.get("workspace_key", "default"))
        app_version = str(metadata.get("app_version", request.revision_id or "unknown"))
        run_topic = str(metadata.get("run_topic", ""))
        migration_enabled = bool(metadata.get("migration_enabled", False))
        migration_revision = str(metadata.get("migration_revision", "head"))

        missing = [
            name
            for name, value in (
                ("worker_id", request.worker_id),
                ("application_id", request.application_id),
                ("deployment_id", request.deployment_id),
                ("target", target),
            )
            if not value
        ]
        if missing:
            return worker_pb2.RegisterWorkerResponse(
                accepted=False,
                reason=f"missing worker registration fields: {', '.join(missing)}",
            )

        capabilities = _message_dict(request.capabilities)
        capabilities["graphs"] = metadata.get("graphs", [])
        await self._upsert_worker(
            request=request,
            metadata=metadata,
            capabilities=capabilities,
            status="registering",
        )

        migration_status = "not_required"
        if migration_enabled:
            if self._coordinator is None:
                await self._set_worker_status(request.worker_id, "failed")
                return worker_pb2.RegisterWorkerResponse(
                    accepted=False,
                    reason="application migration requires UR_DATABASE_URL on Gateway",
                )
            claim = await self._coordinator.claim(
                application_id=request.application_id,
                workspace_key=workspace_key,
                environment=self._environment,
                app_version=app_version,
                target_revision=migration_revision,
                worker_id=request.worker_id,
            )
            migration_status = claim.decision
            if claim.decision == "migrate":
                await self._set_worker_status(request.worker_id, "migrating")
                error = await self._migrate_worker(target, request, workspace_key, app_version)
                await self._coordinator.complete(claim, success=error is None, error=error)
                if error is not None:
                    await self._set_worker_status(request.worker_id, "failed")
                    return worker_pb2.RegisterWorkerResponse(accepted=False, reason=error)
                migration_status = "success"
            elif claim.decision == "wait":
                state = await self._coordinator.wait_for_completion(
                    claim,
                    timeout_seconds=self._migration_timeout_seconds,
                )
                if state.status != "success":
                    await self._set_worker_status(request.worker_id, "failed")
                    return worker_pb2.RegisterWorkerResponse(
                        accepted=False,
                        reason=state.error or "application migration failed on another worker",
                    )
                migration_status = "success"
            else:
                migration_status = "already_current"

        await self._set_worker_status(request.worker_id, "ready")
        defaults = struct_pb2.Struct()
        defaults.update(
            {
                "environment": self._environment,
                "run_topic": run_topic,
                "migration_status": migration_status,
            }
        )
        return worker_pb2.RegisterWorkerResponse(
            accepted=True,
            heartbeat_interval_seconds=int(os.environ.get("UR_WORKER_HEARTBEAT_SECONDS", "15")),
            resolved_execution_defaults=defaults,
        )

    async def Work(  # noqa: N802
        self,
        request_iterator: AsyncIterator[worker_pb2.WorkerMessage],
        context: grpc.aio.ServicerContext,
    ) -> AsyncIterator[worker_pb2.ControllerMessage]:
        del context
        async for message in request_iterator:
            if not message.HasField("heartbeat"):
                continue
            heartbeat = message.heartbeat
            await self._heartbeat(
                heartbeat.worker_id,
                active_executions=heartbeat.active_executions,
                available_slots=heartbeat.available_slots,
            )
            update = struct_pb2.Struct()
            update.update(
                {
                    "worker_id": heartbeat.worker_id,
                    "accepted": True,
                    "environment": self._environment,
                }
            )
            yield worker_pb2.ControllerMessage(config_update=update)

    async def _migrate_worker(
        self,
        target: str,
        request: worker_pb2.RegisterWorkerRequest,
        workspace_key: str,
        app_version: str,
    ) -> str | None:
        channel = grpc.aio.insecure_channel(target)
        try:
            stub = worker_pb2_grpc.WorkerControlServiceStub(channel)
            response = await stub.Migrate(
                worker_pb2.MigrateRequest(
                    application_id=request.application_id,
                    workspace_key=workspace_key,
                    environment=self._environment,
                    app_version=app_version,
                ),
                timeout=self._migration_timeout_seconds,
            )
            return None if response.success else response.error or "application migration failed"
        except Exception as exc:
            return f"worker migration RPC failed: {exc}"
        finally:
            await channel.close()

    async def _upsert_worker(
        self,
        *,
        request: worker_pb2.RegisterWorkerRequest,
        metadata: dict[str, Any],
        capabilities: dict[str, Any],
        status: str,
    ) -> None:
        record = {
            "worker_id": request.worker_id,
            "workspace_key": str(metadata.get("workspace_key", "default")),
            "application_id": request.application_id,
            "revision_id": request.revision_id or "unknown",
            "deployment_id": request.deployment_id,
            "target": str(metadata.get("target", "")),
            "pod_name": request.pod_name,
            "app_version": str(metadata.get("app_version", request.revision_id or "unknown")),
            "run_topic": str(metadata.get("run_topic", "")),
            "max_concurrency": request.max_concurrency,
            "config_hash": request.config_hash,
            "status": status,
            "capabilities": capabilities,
        }
        self._local_workers[request.worker_id] = record
        if self._engine is None:
            return
        async with self._engine.begin() as connection:
            await connection.execute(
                text(
                    """
                    INSERT INTO rt_exec.workers (
                        id, worker_id, workspace_key, application_id, revision_id,
                        deployment_id, target, pod_name, app_version, run_topic,
                        max_concurrency, config_hash, status, capabilities,
                        created_at, updated_at
                    ) VALUES (
                        :worker_id, :worker_id, :workspace_key, :application_id, :revision_id,
                        :deployment_id, :target, :pod_name, :app_version, :run_topic,
                        :max_concurrency, :config_hash, :status, CAST(:capabilities AS json),
                        NOW(), NOW()
                    )
                    ON CONFLICT (worker_id) DO UPDATE SET
                        workspace_key = EXCLUDED.workspace_key,
                        application_id = EXCLUDED.application_id,
                        revision_id = EXCLUDED.revision_id,
                        deployment_id = EXCLUDED.deployment_id,
                        target = EXCLUDED.target,
                        pod_name = EXCLUDED.pod_name,
                        app_version = EXCLUDED.app_version,
                        run_topic = EXCLUDED.run_topic,
                        max_concurrency = EXCLUDED.max_concurrency,
                        config_hash = EXCLUDED.config_hash,
                        status = EXCLUDED.status,
                        capabilities = EXCLUDED.capabilities,
                        updated_at = NOW()
                    """
                ),
                {**record, "capabilities": json.dumps(capabilities, separators=(",", ":"))},
            )

    async def _set_worker_status(self, worker_id: str, status: str) -> None:
        if worker_id in self._local_workers:
            self._local_workers[worker_id]["status"] = status
        if self._engine is None:
            return
        async with self._engine.begin() as connection:
            await connection.execute(
                text(
                    """
                    UPDATE rt_exec.workers
                    SET status = :status, updated_at = NOW()
                    WHERE worker_id = :worker_id
                    """
                ),
                {"worker_id": worker_id, "status": status},
            )

    async def _heartbeat(
        self,
        worker_id: str,
        *,
        active_executions: int,
        available_slots: int,
    ) -> None:
        now = datetime.now(UTC).isoformat()
        if worker_id in self._local_workers:
            self._local_workers[worker_id]["status"] = "ready"
            self._local_workers[worker_id]["last_heartbeat_at"] = now
            self._local_workers[worker_id]["active_executions"] = active_executions
            self._local_workers[worker_id]["available_slots"] = available_slots
        if self._engine is None:
            return
        async with self._engine.begin() as connection:
            await connection.execute(
                text(
                    """
                    UPDATE rt_exec.workers
                    SET status = 'ready', last_heartbeat_at = NOW(), updated_at = NOW()
                    WHERE worker_id = :worker_id
                    """
                ),
                {"worker_id": worker_id},
            )


def attach_worker_control(app: FastAPI) -> FastAPI:
    """Attach the internal gRPC control plane to a Gateway FastAPI app."""

    app.state.migration_done = False
    app.router.routes[:] = [
        route
        for route in app.router.routes
        if getattr(route, "path", None)
        not in {"/internal/workers/register", "/internal/workers"}
    ]

    @app.on_event("startup")
    async def start_worker_control() -> None:
        database_url = os.environ.get("UR_DATABASE_URL")
        engine = create_engine(database_url) if database_url else None
        environment = os.environ.get("UR_KAFKA_ENVIRONMENT", "local")
        server = grpc.aio.server()
        worker_pb2_grpc.add_WorkerControlServiceServicer_to_server(
            GatewayWorkerControlServicer(
                engine,
                environment=environment,
                migration_timeout_seconds=float(
                    os.environ.get("UR_APPLICATION_MIGRATION_TIMEOUT_SECONDS", "300")
                ),
            ),
            server,
        )
        health_servicer = health.HealthServicer()
        health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)
        health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
        port = int(os.environ.get("UR_GATEWAY_CONTROL_GRPC_PORT", "9092"))
        server.add_insecure_port(f"0.0.0.0:{port}")
        await server.start()
        app.state.worker_control_server = server
        app.state.worker_control_engine = engine

    @app.on_event("shutdown")
    async def stop_worker_control() -> None:
        server = getattr(app.state, "worker_control_server", None)
        if server is not None:
            await server.stop(5)
        engine = getattr(app.state, "worker_control_engine", None)
        if engine is not None:
            await engine.dispose()

    return app


def _message_dict(message: Any) -> dict[str, Any]:
    return dict(
        json_format.MessageToDict(
            message,
            preserving_proto_field_name=True,
        )
    )
