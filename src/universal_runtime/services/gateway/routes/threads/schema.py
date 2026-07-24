from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from fastapi.routing import APIRoute
from pydantic import BaseModel, ConfigDict, Field

from universal_runtime.services.gateway.routes.schema import (
    JsonListResponse,
    JsonObjectResponse,
    PruneResponse,
    ThreadResponse,
)


class ThreadSearchRequest(BaseModel):
    metadata: dict[str, Any] = Field(default_factory=dict)
    status: str | None = None
    limit: int = Field(default=10, ge=0, le=1000)
    offset: int = Field(default=0, ge=0)

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "metadata": {"customer_id": "customer-42"},
                    "status": "idle",
                    "limit": 20,
                    "offset": 0,
                }
            ]
        }
    )


class ThreadUpdateRequest(BaseModel):
    metadata: dict[str, Any]

    model_config = ConfigDict(
        json_schema_extra={"examples": [{"metadata": {"priority": "high"}}]}
    )


class ThreadStateUpdateRequest(BaseModel):
    values: dict[str, Any]
    as_node: str | None = None

    model_config = ConfigDict(
        json_schema_extra={"examples": [{"values": {"messages": []}}]}
    )


def response_model(route: APIRoute) -> Any | None:
    if route.path.endswith("/history"):
        return JsonListResponse
    if "/state" in route.path:
        return JsonObjectResponse
    if route.path.endswith("/count"):
        return int
    if route.path.endswith("/search"):
        return list[ThreadResponse]
    if route.path.endswith("/prune"):
        return PruneResponse
    if "DELETE" in route.methods or route.status_code == 204:
        return None
    return ThreadResponse


def examples(route: APIRoute) -> Mapping[str, Any]:
    if route.path == "/threads":
        return {
            "create": {
                "summary": "Create a conversation thread",
                "value": {
                    "thread_id": "thread-01",
                    "metadata": {"customer_id": "customer-42"},
                },
            }
        }
    if route.path.endswith("/search"):
        return {
            "filter": {
                "value": {
                    "metadata": {"customer_id": "customer-42"},
                    "status": "idle",
                    "limit": 20,
                    "offset": 0,
                }
            }
        }
    if route.path.endswith("/state") and "POST" in route.methods:
        return {"update": {"value": {"values": {"messages": []}}}}
    if route.path.endswith("/prune"):
        return {"prune": {"value": {"thread_ids": ["thread-01", "thread-02"]}}}
    if "PATCH" in route.methods or "PUT" in route.methods:
        return {"metadata": {"value": {"metadata": {"priority": "high"}}}}
    return {}
