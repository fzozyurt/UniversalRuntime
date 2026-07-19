from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class RouteDescriptor:
    path: str
    methods: tuple[str, ...]
    name: str | None = None


@dataclass(frozen=True, slots=True)
class AsgiApplicationDescriptor:
    entrypoint: str
    framework: str
    object_kind: str
    routes: tuple[RouteDescriptor, ...]
    has_lifespan: bool
    docs_paths: tuple[str, ...]
    detection_method: str
    warnings: tuple[str, ...]

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def describe_application(
    application: Any, *, entrypoint: str, detection_method: str
) -> AsgiApplicationDescriptor:
    routes: list[RouteDescriptor] = []
    for route in getattr(application, "routes", ()):
        path = getattr(route, "path", None)
        if not isinstance(path, str):
            continue
        methods = tuple(sorted(getattr(route, "methods", ()) or ()))
        routes.append(RouteDescriptor(path, methods, getattr(route, "name", None)))
    docs = tuple(
        path
        for path in ("/docs", "/redoc", "/openapi.json")
        if any(item.path == path for item in routes)
    )
    return AsgiApplicationDescriptor(
        entrypoint=entrypoint,
        framework=type(application).__module__.split(".")[0],
        object_kind="application",
        routes=tuple(routes),
        has_lifespan=bool(getattr(application, "router", None)),
        docs_paths=docs,
        detection_method=detection_method,
        warnings=(),
    )
