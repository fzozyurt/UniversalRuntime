from __future__ import annotations

from fastapi import APIRouter

from universal_runtime.adapters.fastapi.router_registry import (
    RouteContract,
    RouterContext,
    extract_routes,
)
from universal_runtime.services.gateway.routes.runs import schema

AUTO_PREFIX = False


def _is_run_route(path: str) -> bool:
    return path == "/runs" or path.startswith("/runs/") or "/runs" in path or path.startswith(
        "/api/v1/runs/"
    )


def build_router(context: RouterContext, tag: str) -> APIRouter:
    return extract_routes(
        context,
        tag=tag,
        contract=RouteContract(
            predicate=lambda route: _is_run_route(route.path),
            response_model=schema.response_model,
            examples=schema.examples,
        ),
    )
