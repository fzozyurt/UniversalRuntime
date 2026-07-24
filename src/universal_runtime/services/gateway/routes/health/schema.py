from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from fastapi.routing import APIRoute

from universal_runtime.services.gateway.routes.schema import (
    HealthResponse,
    JsonObjectResponse,
    ReadinessResponse,
)


def response_model(route: APIRoute) -> Any | None:
    if route.path == "/ok":
        return HealthResponse
    if route.path == "/ready":
        return ReadinessResponse
    if route.path == "/info":
        return JsonObjectResponse
    return None


def examples(route: APIRoute) -> Mapping[str, Any]:
    del route
    return {}
