from __future__ import annotations

from fastapi import APIRouter

from universal_runtime.adapters.fastapi.router_registry import (
    RouteContract,
    RouterContext,
    extract_routes,
)
from universal_runtime.services.gateway.routes.assistants import schema

AUTO_PREFIX = False


def build_router(context: RouterContext, tag: str) -> APIRouter:
    return extract_routes(
        context,
        tag=tag,
        contract=RouteContract(
            predicate=lambda route: route.path == "/assistants"
            or route.path.startswith("/assistants/"),
            response_model=schema.response_model,
            examples=schema.examples,
        ),
    )
