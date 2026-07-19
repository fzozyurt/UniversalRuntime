from __future__ import annotations

import re
from pathlib import Path

from universal_runtime.adapters.fastapi.ast_scanner import discover_fastapi_entrypoints
from universal_runtime.adapters.fastapi.descriptor import AsgiApplicationDescriptor
from universal_runtime.adapters.fastapi.errors import invalid_entrypoint
from universal_runtime.domain.errors import ErrorCode, RuntimeFailure

_ENTRYPOINT = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*:[A-Za-z_][A-Za-z0-9_]*$")


def detect_asgi_application(
    root: Path, *, explicit_entrypoint: str | None = None, isolated_import: bool = False
) -> AsgiApplicationDescriptor:
    if explicit_entrypoint:
        _validate_entrypoint(explicit_entrypoint)
        return AsgiApplicationDescriptor(
            explicit_entrypoint, "asgi", "application", (), True, (), "explicit", ()
        )
    candidates = discover_fastapi_entrypoints(root)
    if len(candidates) == 1:
        return AsgiApplicationDescriptor(
            candidates[0], "fastapi", "application", (), True, (), "ast", ()
        )
    if len(candidates) > 1:
        raise RuntimeFailure(
            ErrorCode.CUSTOM_HTTP_DISCOVERY_AMBIGUOUS,
            "multiple ASGI applications were discovered",
            details={"candidates": list(candidates)},
        )
    if isolated_import:
        from universal_runtime.adapters.fastapi.isolated_inspector import inspect_isolated

        return inspect_isolated(root)
    raise RuntimeFailure(ErrorCode.CUSTOM_HTTP_UNAVAILABLE, "no ASGI application was discovered")


def _validate_entrypoint(entrypoint: str) -> None:
    if _ENTRYPOINT.fullmatch(entrypoint) is None:
        raise invalid_entrypoint(entrypoint)
