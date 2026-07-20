from __future__ import annotations

from datetime import UTC, datetime

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from universal_runtime.domain.assistants import Assistant
from universal_runtime.domain.errors import RuntimeFailure
from universal_runtime.domain.identity import AssistantId, WorkerId
from universal_runtime.domain.workers import WorkerRegistration
from universal_runtime.services.gateway.worker_routes import (
    create_worker_registry_router,
)


class CapturingCatalog:
    def __init__(self) -> None:
        self.registration = None

    async def register(self, registration):
        self.registration = registration
        return (
            Assistant(
                assistant_id=AssistantId.parse(
                    f"{registration.application_id}:graph-1"
                ),
                graph_id="graph-1",
            ),
        )


class CapturingRegistry:
    def __init__(self) -> None:
        self.registration: WorkerRegistration | None = None

    async def upsert(
        self,
        registration: WorkerRegistration,
    ) -> WorkerRegistration:
        self.registration = registration
        return registration

    async def candidates(self, deployment_id, graph_id, *, now):
        del deployment_id, graph_id, now
        return (
            (self.registration,)
            if self.registration is not None
            else ()
        )

    async def get(self, worker_id: WorkerId):
        del worker_id
        assert self.registration is not None
        return self.registration

    async def acquire(self, *args, **kwargs):
        del args, kwargs
        raise NotImplementedError

    async def active_lease(self, *args, **kwargs):
        del args, kwargs
        return None

    async def release(self, *args, **kwargs):
        del args, kwargs

    async def mark_draining(self, *args, **kwargs):
        del args, kwargs

    async def expire(self, *, now: datetime) -> int:
        del now
        return 0


def _client(catalog: CapturingCatalog, registry: CapturingRegistry) -> TestClient:
    app = FastAPI()
    app.include_router(create_worker_registry_router(registry, catalog))
    return TestClient(app)


def test_worker_registration_maps_catalog_and_capacity_contract() -> None:
    catalog = CapturingCatalog()
    registry = CapturingRegistry()
    client = _client(catalog, registry)

    response = client.post(
        "/internal/workers/register",
        json={
            "worker_id": "worker-1",
            "grpc_target": "worker-1:9090",
            "workspace_id": "workspace",
            "project_id": "project",
            "application_id": "application",
            "application_name": "Application",
            "revision_id": "revision-7",
            "deployment_id": "deployment-blue",
            "environment": "test",
            "image_digest": "sha256:abc",
            "activate_revision": True,
            "graphs": [
                {
                    "graph_id": "graph-1",
                    "entrypoint": "application.graph:build",
                    "descriptor": {"profile": "langgraph"},
                }
            ],
            "max_concurrency": 8,
            "active_executions": 2,
            "available_slots": 6,
            "status": "ready",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["worker_id"] == "worker-1"
    assert body["graph_ids"] == ["graph-1"]
    assert body["default_assistant_ids"] == ["application:graph-1"]
    assert catalog.registration is not None
    assert catalog.registration.activate_revision is True
    assert catalog.registration.graphs[0].entrypoint == "application.graph:build"
    assert registry.registration is not None
    assert registry.registration.max_concurrency == 8
    assert registry.registration.active_executions == 2
    assert registry.registration.available_slots == 6
    assert registry.registration.last_heartbeat_at.tzinfo is not None
    assert registry.registration.expires_at > datetime.now(UTC)


def test_worker_registration_rejects_untyped_capacity_values() -> None:
    catalog = CapturingCatalog()
    registry = CapturingRegistry()
    client = _client(catalog, registry)

    with pytest.raises(RuntimeFailure, match="max_concurrency"):
        client.post(
            "/internal/workers/register",
            json={
                "worker_id": "worker-1",
                "grpc_target": "worker-1:9090",
                "graph_ids": ["graph-1"],
                "max_concurrency": "eight",
            },
        )
