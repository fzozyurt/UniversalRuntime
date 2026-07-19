from __future__ import annotations

import importlib
from typing import Any

from universal_runtime.adapters.fastapi.detector import _ENTRYPOINT
from universal_runtime.adapters.fastapi.errors import invalid_entrypoint
from universal_runtime.domain.errors import ErrorCode, RuntimeFailure


def load_asgi(entrypoint: str) -> Any:
    if _ENTRYPOINT.fullmatch(entrypoint) is None:
        raise invalid_entrypoint(entrypoint)
    module_name, attribute = entrypoint.split(":", 1)
    try:
        value = getattr(importlib.import_module(module_name), attribute)
    except (ImportError, AttributeError) as exc:
        raise RuntimeFailure(
            ErrorCode.CUSTOM_HTTP_ENTRYPOINT_INVALID, "ASGI entrypoint could not be loaded"
        ) from exc
    if not callable(value):
        raise RuntimeFailure(
            ErrorCode.CUSTOM_HTTP_ENTRYPOINT_INVALID, "ASGI entrypoint is not callable"
        )
    return value
