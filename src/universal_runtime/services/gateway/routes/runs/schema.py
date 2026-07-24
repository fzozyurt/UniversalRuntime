from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from fastapi.routing import APIRoute
from pydantic import BaseModel, ConfigDict, Field

from universal_runtime.services.gateway.routes.schema import JsonObjectResponse, RunResponse


class RunCreateRequest(BaseModel):
    assistant_id: str
    input: Any | None = None
    command: dict[str, Any] | None = None
    config: dict[str, Any] = Field(default_factory=dict)
    context: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    stream_mode: str | list[str] = "values"
    stream_subgraphs: bool = False
    multitask_strategy: str = "reject"

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "assistant_id": "support-agent",
                    "input": {"messages": [{"role": "user", "content": "Hello"}]},
                    "stream_mode": ["values", "messages"],
                    "metadata": {"request_source": "web"},
                },
                {
                    "assistant_id": "approval-agent",
                    "command": {"resume": "approved"},
                    "stream_mode": "values",
                },
            ]
        }
    )


class RunCancelManyRequest(BaseModel):
    run_ids: list[str] = Field(default_factory=list)

    model_config = ConfigDict(json_schema_extra={"examples": [{"run_ids": ["run-01", "run-02"]}]})


def response_model(route: APIRoute) -> Any | None:
    if route.path.endswith("/stream") or route.path.endswith("/events"):
        return None
    if route.path.endswith("/join") or route.path.endswith("/wait"):
        return JsonObjectResponse
    if route.path.endswith("/batch"):
        return list[RunResponse]
    if route.path.endswith("/cancel") or "DELETE" in route.methods:
        return None
    if route.path.endswith("/runs") and "GET" in route.methods:
        return list[RunResponse]
    return RunResponse


def examples(route: APIRoute) -> Mapping[str, Any]:
    if "POST" not in route.methods:
        return {}
    if route.path.endswith("/cancel"):
        return {"cancel": {"value": {"run_ids": ["run-01", "run-02"]}}}
    if route.path.endswith("/batch"):
        return {
            "batch": {
                "value": [
                    {"assistant_id": "support-agent", "input": {"question": "One"}},
                    {"assistant_id": "support-agent", "input": {"question": "Two"}},
                ]
            }
        }
    if "/runs" in route.path:
        return {
            "invoke": {
                "summary": "Start a run",
                "value": {
                    "assistant_id": "support-agent",
                    "input": {"messages": [{"role": "user", "content": "Hello"}]},
                    "stream_mode": "values",
                },
            },
            "resume": {
                "summary": "Resume an interrupted run",
                "value": {
                    "assistant_id": "approval-agent",
                    "command": {"resume": "approved"},
                    "stream_mode": "values",
                },
            },
        }
    return {}
