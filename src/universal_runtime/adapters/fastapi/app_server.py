from __future__ import annotations

from typing import Any

from universal_runtime.adapters.fastapi.descriptor import (
    AsgiApplicationDescriptor,
    describe_application,
)
from universal_runtime.adapters.fastapi.loader import load_asgi
from universal_runtime.adapters.fastapi.middleware import RequestContextMiddleware
from universal_runtime.adapters.fastapi.root_path import RootPathMiddleware


def load_application(entrypoint: str, *, root_path: str = "") -> Any:
    application = RequestContextMiddleware(load_asgi(entrypoint))
    return RootPathMiddleware(application, root_path)


def load_application_with_descriptor(
    entrypoint: str, *, root_path: str = ""
) -> tuple[Any, AsgiApplicationDescriptor]:
    raw = load_asgi(entrypoint)
    descriptor = describe_application(raw, entrypoint=entrypoint, detection_method="import")
    application = RootPathMiddleware(RequestContextMiddleware(raw), root_path)
    return application, descriptor
