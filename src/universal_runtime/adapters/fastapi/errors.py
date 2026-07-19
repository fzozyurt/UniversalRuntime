from __future__ import annotations

from universal_runtime.domain.errors import ErrorCode, RuntimeFailure


def invalid_entrypoint(entrypoint: str) -> RuntimeFailure:
    return RuntimeFailure(
        ErrorCode.CUSTOM_HTTP_ENTRYPOINT_INVALID,
        "ASGI entrypoint must use module.path:attribute syntax",
        details={"entrypoint": entrypoint},
    )
