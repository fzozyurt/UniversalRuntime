from __future__ import annotations

from copy import deepcopy

import pytest
from fastapi.testclient import TestClient

from universal_runtime.configuration.precedence import merge_config_precedence
from universal_runtime.services.gateway.app import create_app


def runtime_config(name: str = "demo") -> dict[str, object]:
    return {
        "apiVersion": "runtime.ai/v1alpha1",
        "kind": "RuntimeApplication",
        "metadata": {"name": name},
        "spec": {
            "runtime": {
                "adapter": "langgraph",
                "entrypoints": {"graphs": {"default": "application:graph"}},
            }
        },
    }


def test_gateway_compatibility_resources_are_not_native_enveloped() -> None:
    client = TestClient(create_app())
    assert client.get("/ok").json() == {"ok": True}

    assistant = client.post("/assistants", json={"graph_id": "default"}).json()
    thread = client.post("/threads", json={}).json()
    run = client.post(
        f"/threads/{thread['thread_id']}/runs",
        json={"assistant_id": assistant["assistant_id"], "input": {"value": 1}},
    ).json()

    assert set(assistant) >= {"assistant_id", "graph_id", "version"}
    assert set(thread) >= {"thread_id", "status"}
    assert set(run) >= {"run_id", "assistant_id", "status"}
    assert "data" not in assistant
    assert "data" not in thread
    assert "data" not in run

    cancelled = client.post(f"/runs/{run['run_id']}/cancel")
    assert cancelled.status_code == 204
    assert client.get("/assistants/nonexistent").status_code == 404
    assert client.post("/assistants/search").json()[0]["assistant_id"] == assistant["assistant_id"]


def test_native_config_validate_revision_hash_and_activation() -> None:
    client = TestClient(create_app())
    config = runtime_config()
    validation = client.post("/api/v1/applications/app/config/validate", json=config)
    assert validation.status_code == 200
    assert validation.json()["data"]["valid"] is True
    assert "meta" in validation.json()

    first = client.put("/api/v1/applications/app/config", json=config)
    second_config = deepcopy(config)
    second_config["metadata"] = {"name": "demo-two"}
    second = client.put("/api/v1/applications/app/config", json=second_config)
    assert first.status_code == 201
    assert second.status_code == 201
    assert first.json()["data"]["revision"] == 1
    assert second.json()["data"]["revision"] == 2
    assert first.json()["data"]["config_hash"] != second.json()["data"]["config_hash"]

    fetched = client.get("/api/v1/applications/app/config").json()["data"]
    assert fetched["revision"] == 1
    activated = client.post("/api/v1/applications/app/config/2/activate")
    assert activated.json()["data"]["active"] is True
    assert client.get("/api/v1/applications/app/config").json()["data"]["revision"] == 2


def test_native_invalid_config_uses_typed_error() -> None:
    client = TestClient(create_app())
    response = client.put("/api/v1/applications/app/config", json={"metadata": {}})
    assert response.status_code == 422
    body = response.json()
    assert body["error"]["code"] == "INVALID_EXECUTION_INPUT"
    assert "data" not in body


def test_config_precedence_is_low_to_high_and_defensive() -> None:
    defaults = {"runtime": {"timeout": 10, "mode": "local"}}
    deployment = {"runtime": {"timeout": 20}}
    policy = {"runtime": {"mode": "restricted"}}
    result = merge_config_precedence(defaults, deployment, policy)
    assert result == {"runtime": {"timeout": 20, "mode": "restricted"}}
    deployment["runtime"]["timeout"] = 99
    assert result["runtime"]["timeout"] == 20


@pytest.mark.asyncio
async def test_config_revision_repository_is_defensive() -> None:
    from universal_runtime.adapters.memory.configuration import (
        InMemoryApplicationConfigRepository,
    )

    repository = InMemoryApplicationConfigRepository()
    revision = await repository.create_revision("app", runtime_config())
    revision.config["metadata"]["name"] = "mutated"
    active = await repository.get_active("app")
    assert active.config["metadata"]["name"] == "demo"
