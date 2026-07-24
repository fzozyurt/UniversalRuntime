from __future__ import annotations

import os

from fastapi import FastAPI

from universal_runtime.adapters.postgres.database import create_engine
from universal_runtime.adapters.postgres.migration import migrate_platform


def install_gateway_lifecycle(app: FastAPI) -> None:
    """Replace the compatibility factory lifecycle with the current architecture."""

    app.router.on_startup[:] = [
        callback
        for callback in app.router.on_startup
        if getattr(callback, "__name__", "") != "start_local_execution"
    ]
    app.router.on_shutdown[:] = [
        callback
        for callback in app.router.on_shutdown
        if getattr(callback, "__name__", "") != "stop_local_execution"
    ]
    app.state.migration_done = False

    @app.on_event("startup")
    async def start_runtime() -> None:
        from universal_runtime.services.gateway.compat_app import _auto_register_application

        database_url = os.environ.get("UR_DATABASE_URL")
        if database_url:
            engine = create_engine(database_url)
            try:
                app.state.migration_done = await migrate_platform(
                    engine,
                    application_id=os.environ.get("UR_APPLICATION_ID", "default"),
                    environment=os.environ.get("UR_KAFKA_ENVIRONMENT", "local"),
                )
            finally:
                await engine.dispose()
        await _auto_register_application(app.state.runtime, app)
        await app.state.runtime.start()

    @app.on_event("shutdown")
    async def stop_runtime() -> None:
        await app.state.runtime.shutdown()
        context = getattr(app.state, "langgraph_context", None)
        if context is not None:
            await context.__aexit__(None, None, None)
        migration_engine = getattr(app.state, "langgraph_migration_engine", None)
        if migration_engine is not None:
            await migration_engine.dispose()
