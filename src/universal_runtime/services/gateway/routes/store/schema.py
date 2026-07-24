from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from fastapi.routing import APIRoute
from pydantic import BaseModel, ConfigDict, Field

from universal_runtime.services.gateway.routes.schema import JsonObjectResponse


class StoreItemRequest(BaseModel):
    namespace: list[str]
    key: str
    value: dict[str, Any] = Field(default_factory=dict)
    index: bool | list[str] | None = None

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "namespace": ["customers", "customer-42"],
                    "key": "profile",
                    "value": {"language": "tr", "tier": "pro"},
                    "index": ["language", "tier"],
                }
            ]
        }
    )


class StoreSearchRequest(BaseModel):
    namespace_prefix: list[str] = Field(default_factory=list)
    filter: dict[str, Any] | None = None
    limit: int = Field(default=10, ge=0, le=1000)
    offset: int = Field(default=0, ge=0)


class NamespaceSearchRequest(BaseModel):
    prefix: list[str] | None = None
    suffix: list[str] | None = None
    max_depth: int | None = Field(default=None, ge=0)
    limit: int = Field(default=100, ge=0, le=1000)
    offset: int = Field(default=0, ge=0)


def response_model(route: APIRoute) -> Any | None:
    if route.status_code == 204 or "DELETE" in route.methods or "PUT" in route.methods:
        return None
    return JsonObjectResponse


def examples(route: APIRoute) -> Mapping[str, Any]:
    if route.path == "/store/items" and "PUT" in route.methods:
        return {
            "put": {
                "value": {
                    "namespace": ["customers", "customer-42"],
                    "key": "profile",
                    "value": {"language": "tr", "tier": "pro"},
                    "index": ["language", "tier"],
                }
            }
        }
    if route.path.endswith("/search"):
        return {
            "search": {
                "value": {
                    "namespace_prefix": ["customers"],
                    "filter": {"tier": "pro"},
                    "limit": 10,
                    "offset": 0,
                }
            }
        }
    if route.path.endswith("/namespaces"):
        return {"list": {"value": {"prefix": ["customers"], "max_depth": 3}}}
    return {}
