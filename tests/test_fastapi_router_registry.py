from __future__ import annotations

import pytest
from fastapi import FastAPI

from universal_runtime.adapters.fastapi.router_registry import (
    RouterContext,
    finalize_route_metadata,
    register_router_package,
    validate_openapi_contract,
)
from universal_runtime.bootstrap.local import create_local_runtime
from universal_runtime.domain.identity import AssistantId, RunId, ThreadId
from universal_runtime.services.gateway.app import create_app
from universal_runtime.services.gateway.scope import deployment_identity


def test_folder_router_registration_generates_paths_tags_and_operation_ids() -> None:
    app = FastAPI()
    register_router_package(
        app,
        "tests.fixtures.auto_api",
        context=RouterContext(app=app),
    )
    finalize_route_metadata(app)
    validate_openapi_contract(app)

    document = app.openapi()
    create = document["paths"]["/assistants/"]["post"]
    history = document["paths"]["/assistants/history/"]["get"]

    assert create["tags"] == ["Assistants"]
    assert create["operationId"] == "post_assistants"
    assert create["requestBody"]["content"]["application/json"]["examples"]
    assert create["responses"]["200"]["content"]["application/json"]["schema"]
    assert history["tags"] == ["Assistants / History"]
    assert history["operationId"] == "get_assistants_history"


def test_gateway_router_metadata_preserves_langgraph_sdk_paths() -> None:
    app = create_app(create_local_runtime())
    document = app.openapi()

    assistant_create = document["paths"]["/assistants"]["post"]
    run_stream = document["paths"]["/threads/{thread_id}/runs/stream"]["post"]

    assert assistant_create["tags"] == ["Assistants"]
    assert assistant_create["operationId"] == "post_assistants"
    assert assistant_create["requestBody"]["content"]["application/json"]["examples"]
    assert run_stream["tags"] == ["Runs"]
    assert "/internal/workers/register" not in document["paths"]


def test_deployment_identity_uses_runtime_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("UR_WORKSPACE_ID", "workspace-1")
    monkeypatch.setenv("UR_PROJECT_ID", "project-1")
    monkeypatch.setenv("UR_APPLICATION_ID", "application-1")
    monkeypatch.setenv("UR_REVISION_ID", "revision-1")
    monkeypatch.setenv("UR_DEPLOYMENT_ID", "deployment-1")

    identity = deployment_identity(
        AssistantId.parse("assistant-1"),
        RunId.parse("run-1"),
        ThreadId.parse("thread-1"),
    )

    assert str(identity.workspace_id) == "workspace-1"
    assert str(identity.project_id) == "project-1"
    assert str(identity.application_id) == "application-1"
    assert str(identity.revision_id) == "revision-1"
    assert str(identity.deployment_id) == "deployment-1"
