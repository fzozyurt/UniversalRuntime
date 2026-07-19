from __future__ import annotations

from universal_runtime.domain.errors import ErrorCode, RuntimeFailure


def invalid_context_id(value: str) -> RuntimeFailure:
    return RuntimeFailure(ErrorCode.INVALID_EXECUTION_INPUT, f"invalid A2A context ID: {value}")


def unsupported_part(kind: str) -> RuntimeFailure:
    return RuntimeFailure(
        ErrorCode.CAPABILITY_NOT_SUPPORTED,
        f"A2A message part is not supported: {kind}",
    )
