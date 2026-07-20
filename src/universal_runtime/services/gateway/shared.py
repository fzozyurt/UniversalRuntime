from __future__ import annotations

import os

from fastapi import FastAPI

from universal_runtime.adapters.postgres.control_plane import PostgresControlPlaneCatalog
from universal_runtime.adapters.postgres.database import create_engine, create_session_factory
from universal_runtime.adapters.postgres.workers import PostgresWorkerRegistry
from universal_runtime.services.gateway.app import create_app
from universal_runtime.services.gateway.worker_routes import create_worker_registry_router

_WORKER_ROUTE_PATHS = {"/internal/workers", "/internal/workers/register"}


def create_shared_gateway_app() -> FastAPI:
    """Create the platform Gateway without importing application user code.

    Application inspection, framework persistence and execution remain inside
    Worker/standalone processes. The Gateway owns platform metadata, execution
    records and HA worker registration only.
    """
    if os.environ.get("UR_APPLICATION_ENTRYPOINT") or os.environ.get(
        "UR_APPLICATION_ENTRYPOINTS"
    ):
        raise RuntimeError(
            "shared Gateway must not receive UR_APPLICATION_ENTRYPOINT(S); "
            "configure entrypoints only on Worker or standalone deployments"
        )
    app = create_app()
    database_url = os.environ.get("UR_PLATFORM_DATABASE_URL") or os.environ.get(
        "UR_DATABASE_URL"
    )
    if not database_url:
        raise RuntimeError("shared Gateway requires UR_PLATFORM_DATABASE_URL or UR_DATABASE_URL")
    engine = create_engine(
        database_url,
        pool_size=int(os.environ.get("UR_GATEWAY_DB_POOL_SIZE", "10")),
        max_overflow=int(os.environ.get("UR_GATEWAY_DB_MAX_OVERFLOW", "20")),
    )
    sessions = create_session_factory(engine)
    registry = PostgresWorkerRegistry(sessions)
    catalog = PostgresControlPlaneCatalog(
        sessions,
        environment=os.environ.get("UR_KAFKA_ENVIRONMENT", "local"),
    )
    app.router.routes = [
        route
        for route in app.router.routes
        if getattr(route, "path", None) not in _WORKER_ROUTE_PATHS
    ]
    app.include_router(create_worker_registry_router(registry, catalog))
    app.state.worker_registry_repository = registry
    app.state.control_plane_catalog = catalog
    app.state.control_plane_engine = engine

    @app.on_event("shutdown")
    async def close_control_plane_engine() -> None:
        await engine.dispose()

    return app
