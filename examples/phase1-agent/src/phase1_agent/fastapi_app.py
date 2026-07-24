from __future__ import annotations

from fastapi import FastAPI

from universal_runtime.adapters.fastapi.router_registry import (
    RouterContext,
    finalize_route_metadata,
    register_router_package,
    validate_openapi_contract,
)

app = FastAPI(title="Phase 1 deterministic agent")
register_router_package(
    app,
    "phase1_agent.http",
    context=RouterContext(app=app),
)
finalize_route_metadata(app)
validate_openapi_contract(app)
