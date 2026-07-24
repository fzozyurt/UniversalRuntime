from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from fastapi.routing import APIRoute
from pydantic import BaseModel, ConfigDict, Field

from universal_runtime.services.gateway.routes.schema import (
    AssistantResponse,
    AssistantSchemasResponse,
    JsonObjectResponse,
)


class AssistantSearchRequest(BaseModel):
    graph_id: str | None = None
    name: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    limit: int = Field(default=10, ge=0, le=1000)
    offset: int = Field(default=0, ge=0)

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "graph_id": "support-agent",
                    "metadata": {"team": "support"},
                    "limit": 20,
                    "offset": 0,
                }
            ]
        }
    )


class AssistantVersionRequest(BaseModel):
    limit: int = Field(default=10, ge=0, le=1000)
    offset: int = Field(default=0, ge=0)


class AssistantLatestRequest(BaseModel):
    version: int = Field(ge=1)


def response_model(route: APIRoute) -> Any | None:
    if route.path.endswith("/schemas"):
        return AssistantSchemasResponse
    if route.path.endswith("/graph") or route.path.endswith("/subgraphs"):
        return JsonObjectResponse
    if route.path.endswith("/count"):
        return int
    if route.path.endswith("/search") or route.path.endswith("/versions"):
        return list[AssistantResponse]
    if "DELETE" in route.methods:
        return None
    return AssistantResponse


def examples(route: APIRoute) -> Mapping[str, Any]:
    if route.path == "/assistants":
        return {
            "create": {
                "summary": "Create an assistant",
                "value": {
                    "assistant_id": "support-agent",
                    "graph_id": "support-agent",
                    "name": "Support Agent",
                    "description": "Answers support questions.",
                    "config": {},
                    "context": {},
                    "metadata": {"team": "support"},
                },
            }
        }
    if route.path.endswith("/search"):
        return {
            "filter": {
                "summary": "Filter assistants",
                "value": AssistantSearchRequest.model_config["json_schema_extra"]["examples"][0],
            }
        }
    if route.path.endswith("/versions"):
        return {"page": {"value": {"limit": 10, "offset": 0}}}
    if route.path.endswith("/latest"):
        return {"activate": {"value": {"version": 2}}}
    return {}
