from __future__ import annotations

from fastapi import APIRouter

from universal_runtime.adapters.fastapi.router_registry import (
    RouteContract,
    RouterContext,
    extract_routes,
)
from universal_runtime.services.gateway.routes.health import schema

AUTO_PREFIX = False
_PATHS = {"/ok", "/ready", "/info"}


def build_router(context: RouterContext, tag: str) -> APIRouter:
    return extract_routes(
        context,
        tag=tag,
        contract=RouteContract(
            predicate=lambda route: route.path in _PATHS,
            response_model=schema.response_model,
            examples=schema.examples,
        ),
    )
