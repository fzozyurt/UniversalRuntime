from __future__ import annotations

import importlib
import pkgutil
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import ModuleType
from typing import Any

from fastapi import APIRouter, FastAPI
from fastapi.routing import APIRoute

RoutePredicate = Callable[[APIRoute], bool]
ResponseModelResolver = Callable[[APIRoute], Any | None]
ExamplesResolver = Callable[[APIRoute], Mapping[str, Any]]


@dataclass(frozen=True, slots=True)
class RouterContext:
    app: FastAPI
    runtime: Any


@dataclass(frozen=True, slots=True)
class RouteContract:
    predicate: RoutePredicate
    response_model: ResponseModelResolver
    examples: ExamplesResolver


def register_router_package(
    app: FastAPI,
    package_name: str,
    *,
    context: RouterContext | None = None,
) -> None:
    """Discover every ``routes.py`` below a package and include it deterministically.

    A route module exports ``build_router(context, tag)``. The tag is derived from
    its folder path, so ``routes/assistants/routes.py`` becomes ``Assistants`` and
    ``routes/admin/audit/routes.py`` becomes ``Admin / Audit``. Modules may keep
    protocol paths unchanged; folder prefixes are applied only when the module
    explicitly opts into ``AUTO_PREFIX = True``.
    """

    package = importlib.import_module(package_name)
    modules = sorted(_route_modules(package), key=lambda module: module.__name__)
    effective_context = context or RouterContext(app=app, runtime=app.state.runtime)
    for module in modules:
        tag = folder_tag(module.__name__, package_name)
        builder = getattr(module, "build_router", None)
        if builder is None:
            raise RuntimeError(f"router module has no build_router(): {module.__name__}")
        router = builder(effective_context, tag)
        if not isinstance(router, APIRouter):
            raise TypeError(f"build_router() must return APIRouter: {module.__name__}")
        prefix = folder_prefix(module.__name__, package_name) if module.AUTO_PREFIX else ""
        app.include_router(router, prefix=prefix)


def extract_routes(
    context: RouterContext,
    *,
    tag: str,
    contract: RouteContract,
) -> APIRouter:
    """Move matching routes into a documented router without changing paths."""

    router = APIRouter(tags=[tag])
    source_routes = list(context.app.router.routes)
    for route in source_routes:
        if not isinstance(route, APIRoute) or not contract.predicate(route):
            continue
        context.app.router.routes.remove(route)
        _clone_route(
            router,
            route,
            tag=tag,
            response_model=contract.response_model(route),
            examples=contract.examples(route),
        )
    return router


def folder_tag(module_name: str, package_name: str) -> str:
    relative = module_name.removeprefix(package_name).strip(".")
    folders = relative.split(".")[:-1]
    if not folders:
        raise ValueError(f"routes.py must be inside a folder: {module_name}")
    return " / ".join(_title(folder) for folder in folders)


def folder_prefix(module_name: str, package_name: str) -> str:
    relative = module_name.removeprefix(package_name).strip(".")
    folders = relative.split(".")[:-1]
    return "/" + "/".join(_slug(folder) for folder in folders)


def operation_id(route: APIRoute) -> str:
    method = min(route.methods or {"GET"}).lower()
    path = route.path.strip("/") or "root"
    normalized = re.sub(r"[{}]", "", path)
    normalized = re.sub(r"[^a-zA-Z0-9]+", "_", normalized).strip("_").lower()
    return f"{method}_{normalized}"


def _route_modules(package: ModuleType) -> list[ModuleType]:
    package_path = getattr(package, "__path__", None)
    if package_path is None:
        raise ValueError(f"router package is not a package: {package.__name__}")
    modules: list[ModuleType] = []
    for info in pkgutil.walk_packages(package_path, prefix=f"{package.__name__}."):
        if info.name.endswith(".routes"):
            modules.append(importlib.import_module(info.name))
    return modules


def _clone_route(
    router: APIRouter,
    route: APIRoute,
    *,
    tag: str,
    response_model: Any | None,
    examples: Mapping[str, Any],
) -> None:
    openapi_extra = dict(route.openapi_extra or {})
    if examples:
        request_body = dict(openapi_extra.get("requestBody", {}))
        content = dict(request_body.get("content", {}))
        media = dict(content.get("application/json", {}))
        media["examples"] = dict(examples)
        content["application/json"] = media
        request_body["content"] = content
        openapi_extra["requestBody"] = request_body

    route_operation_id = route.operation_id or operation_id(route)
    router.add_api_route(
        route.path,
        route.endpoint,
        response_model=response_model if response_model is not None else route.response_model,
        status_code=route.status_code,
        tags=[tag],
        dependencies=route.dependencies,
        summary=route.summary or _title(route.name),
        description=route.description or _description(route_operation_id),
        response_description=route.response_description,
        responses=route.responses,
        deprecated=route.deprecated,
        methods=route.methods,
        operation_id=route_operation_id,
        response_class=route.response_class,
        name=route.name,
        route_class_override=type(route),
        callbacks=route.callbacks,
        openapi_extra=openapi_extra or None,
        generate_unique_id_function=route.generate_unique_id_function,
    )


def _title(value: str) -> str:
    return " ".join(part.capitalize() for part in re.split(r"[-_]", value) if part)


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def _description(operation: str) -> str:
    return f"UniversalRuntime endpoint for {operation.replace('_', ' ')}."
