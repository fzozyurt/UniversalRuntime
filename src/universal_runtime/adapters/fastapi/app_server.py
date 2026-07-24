from __future__ import annotations

import os
from typing import Any

from fastapi import FastAPI

from universal_runtime.adapters.fastapi.descriptor import (
    AsgiApplicationDescriptor,
    describe_application,
)
from universal_runtime.adapters.fastapi.loader import load_asgi
from universal_runtime.adapters.fastapi.middleware import RequestContextMiddleware
from universal_runtime.adapters.fastapi.root_path import RootPathMiddleware
from universal_runtime.adapters.fastapi.router_registry import (
    RouterContext,
    finalize_route_metadata,
    register_router_package,
    validate_openapi_contract,
)


def load_application(
    entrypoint: str,
    *,
    root_path: str = "",
    router_package: str | None = None,
) -> Any:
    raw = _load_and_register(entrypoint, router_package=router_package)
    application = RequestContextMiddleware(raw)
    return RootPathMiddleware(application, root_path)


def load_application_with_descriptor(
    entrypoint: str,
    *,
    root_path: str = "",
    router_package: str | None = None,
) -> tuple[Any, AsgiApplicationDescriptor]:
    raw = _load_and_register(entrypoint, router_package=router_package)
    descriptor = describe_application(raw, entrypoint=entrypoint, detection_method="import")
    application = RootPathMiddleware(RequestContextMiddleware(raw), root_path)
    return application, descriptor


def _load_and_register(entrypoint: str, *, router_package: str | None) -> Any:
    # A package-only entrypoint is the convention-based mode. Every nested
    # routes.py is discovered; its folder path becomes prefix and tag.
    if ":" not in entrypoint:
        application = FastAPI(title=_title_from_package(entrypoint), version="1.0.0")
        register_router_package(
            application,
            entrypoint,
            context=RouterContext(app=application),
        )
        finalize_route_metadata(application)
        _validate_when_strict(application)
        return application

    application = load_asgi(entrypoint)
    package = router_package or os.environ.get("UR_FASTAPI_ROUTERS_PACKAGE")
    if package:
        if not isinstance(application, FastAPI):
            raise TypeError("folder router discovery requires a FastAPI application")
        register_router_package(
            application,
            package,
            context=RouterContext(app=application),
        )
    if isinstance(application, FastAPI):
        finalize_route_metadata(application)
        _validate_when_strict(application)
        application.openapi_schema = None
    return application


def _validate_when_strict(application: FastAPI) -> None:
    """Keep application startup available while allowing CI/deploy strict gates.

    Existing third-party or legacy FastAPI applications may not yet document every
    request example. Set ``UR_FASTAPI_OPENAPI_STRICT=true`` in CI or deployment
    validation jobs to make incomplete contracts fatal.
    """

    enabled = os.environ.get("UR_FASTAPI_OPENAPI_STRICT", "false").strip().lower()
    if enabled in {"1", "true", "yes", "on"}:
        validate_openapi_contract(application)


def _title_from_package(package_name: str) -> str:
    leaf = package_name.rsplit(".", 1)[-1]
    return " ".join(part.capitalize() for part in leaf.replace("-", "_").split("_"))
