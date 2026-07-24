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
    runtime: Any | None = None


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

    Every route folder must also expose ``schema.py``. A route module may export
    either ``build_router(context, tag)`` or a direct ``router`` object. Folder
    names become tags and, for direct application routers, URL prefixes. Gateway
    compatibility routers opt out with ``AUTO_PREFIX = False`` so LangGraph SDK
    paths remain unchanged.
    """

    package = importlib.import_module(package_name)
    modules = sorted(_route_modules(package), key=lambda module: module.__name__)
    effective_context = context or RouterContext(
        app=app,
        runtime=getattr(app.state, "runtime", None),
    )
    for module in modules:
        _require_schema_module(module)
        tag = folder_tag(module.__name__, package_name)
        builder = getattr(module, "build_router", None)
        direct_router = getattr(module, "router", None)
        if builder is not None:
            router = builder(effective_context, tag)
            default_auto_prefix = False
            include_tags: list[str] | None = None
        elif isinstance(direct_router, APIRouter):
            router = direct_router
            default_auto_prefix = True
            include_tags = [tag]
        else:
            raise RuntimeError(
                f"router module must export build_router() or router: {module.__name__}"
            )
        if not isinstance(router, APIRouter):
            raise TypeError(f"router contract must return APIRouter: {module.__name__}")
        auto_prefix = bool(getattr(module, "AUTO_PREFIX", default_auto_prefix))
        prefix = folder_prefix(module.__name__, package_name) if auto_prefix else ""
        app.include_router(router, prefix=prefix, tags=include_tags)


def finalize_route_metadata(app: FastAPI) -> None:
    """Apply stable names/tags and a minimal example to every remaining route."""

    seen: set[str] = set()
    for route in app.routes:
        if not isinstance(route, APIRoute):
            continue
        if len(route.tags) > 1:
            route.tags = list(dict.fromkeys(route.tags))
        identifier = route.operation_id or operation_id(route)
        if identifier in seen:
            methods = "_".join(sorted(method.lower() for method in route.methods or {"get"}))
            identifier = f"{identifier}_{methods}"
        seen.add(identifier)
        route.operation_id = identifier
        if not route.tags:
            route.tags = [_tag_from_path(route.path)]
        route.summary = route.summary or _title(route.name)
        route.description = route.description or _description(identifier)
        if route.body_field is not None:
            extra = dict(route.openapi_extra or {})
            request_body = dict(extra.get("requestBody", {}))
            content = dict(request_body.get("content", {}))
            media = dict(content.get("application/json", {}))
            media.setdefault(
                "examples",
                {"default": {"summary": "Request example", "value": {}}},
            )
            content["application/json"] = media
            request_body["content"] = content
            extra["requestBody"] = request_body
            route.openapi_extra = extra


def validate_openapi_contract(app: FastAPI) -> None:
    """Fail application startup when documented endpoint metadata is incomplete."""

    document = app.openapi()
    errors: list[str] = []
    for path, path_item in document.get("paths", {}).items():
        for method, operation in path_item.items():
            if method not in {"get", "post", "put", "patch", "delete", "options", "head"}:
                continue
            label = f"{method.upper()} {path}"
            for field in ("operationId", "summary", "description", "tags"):
                if not operation.get(field):
                    errors.append(f"{label}: missing {field}")
            request_body = operation.get("requestBody")
            if request_body:
                media = request_body.get("content", {}).get("application/json", {})
                if not media.get("schema"):
                    errors.append(f"{label}: request body has no schema")
                if not media.get("examples") and not media.get("example"):
                    errors.append(f"{label}: request body has no example")
    if errors:
        raise RuntimeError("invalid FastAPI/OpenAPI contract:\n" + "\n".join(errors))


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


def _require_schema_module(route_module: ModuleType) -> None:
    schema_module = route_module.__name__.removesuffix(".routes") + ".schema"
    try:
        importlib.import_module(schema_module)
    except ModuleNotFoundError as exc:
        if exc.name == schema_module:
            raise RuntimeError(
                f"every routes.py must have a sibling schema.py: {route_module.__name__}"
            ) from exc
        raise


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


def _tag_from_path(path: str) -> str:
    first = next((part for part in path.split("/") if part and not part.startswith("{")), "General")
    return _title(first)


def _description(operation: str) -> str:
    return f"UniversalRuntime endpoint for {operation.replace('_', ' ')}."
