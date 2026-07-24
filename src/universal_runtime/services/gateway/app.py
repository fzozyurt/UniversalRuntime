from __future__ import annotations

from typing import Any

from fastapi import FastAPI

from universal_runtime.adapters.fastapi.router_registry import (
    RouterContext,
    finalize_route_metadata,
    register_router_package,
)
from universal_runtime.bootstrap.local import LocalRuntime
from universal_runtime.domain.assistants import Assistant
from universal_runtime.domain.capabilities import AdapterManifest
from universal_runtime.ports.runtime_adapter import RuntimeAdapter
from universal_runtime.services.gateway import compat_app
from universal_runtime.services.gateway.lifecycle import install_gateway_lifecycle
from universal_runtime.services.gateway.scope import deployment_identity

_ROUTER_PACKAGE = "universal_runtime.services.gateway.routes"
_INTERNAL_HTTP_PATHS = {"/internal/workers/register", "/internal/workers"}


def create_app(
    runtime: LocalRuntime | None = None,
    *,
    runtime_adapter: RuntimeAdapter | None = None,
    custom_http_target: str | None = None,
    a2a_assistant: Assistant | None = None,
    a2a_manifest: AdapterManifest | None = None,
    a2a_public_url: str = "http://localhost:8080",
) -> FastAPI:
    """Create the LangGraph-compatible Gateway with modular route contracts."""

    # Compatibility handlers still contain the protocol behavior. Their runtime
    # identity dependency is injected here so commands use the active deployment
    # instead of a hard-coded Gateway identity.
    compat_app._identity = deployment_identity  # type: ignore[attr-defined]
    app = compat_app.create_app(
        runtime,
        runtime_adapter=runtime_adapter,
        custom_http_target=custom_http_target,
        a2a_assistant=a2a_assistant,
        a2a_manifest=a2a_manifest,
        a2a_public_url=a2a_public_url,
    )
    app.router.routes[:] = [
        route
        for route in app.router.routes
        if getattr(route, "path", None) not in _INTERNAL_HTTP_PATHS
    ]
    install_gateway_lifecycle(app)
    register_router_package(
        app,
        _ROUTER_PACKAGE,
        context=RouterContext(app=app, runtime=app.state.runtime),
    )
    finalize_route_metadata(app)
    app.openapi_schema = None
    return app


__all__ = ["create_app"]
