from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Any

from .noop import NoopTracer


def span(
    tracer: Any, name: str, *, attributes: dict[str, Any] | None = None
) -> AbstractContextManager[Any]:
    return tracer.start_as_current_span(name, attributes=attributes or {})


def record_failure(
    current_span: Any,
    error: BaseException,
    *,
    error_code: str,
    retryable: bool = False,
    cancelled: bool = False,
) -> None:
    if cancelled:
        return
    current_span.record_exception(error)
    current_span.set_attribute("runtime.error_code", error_code)
    current_span.set_attribute("runtime.retryable", retryable)
    try:
        from opentelemetry.trace import Status, StatusCode

        current_span.set_status(Status(StatusCode.ERROR, error_code))
    except ImportError:
        return


def runtime_run_span(tracer: Any | None, attributes: dict[str, Any]) -> AbstractContextManager[Any]:
    return span(tracer or NoopTracer(), "runtime.run", attributes=attributes)


def current_trace_context() -> tuple[str | None, str | None]:
    try:
        from opentelemetry.trace import get_current_span

        context = get_current_span().get_span_context()
        if not context.is_valid:
            return None, None
        return format(context.trace_id, "032x"), format(context.span_id, "016x")
    except ImportError:
        return None, None


def record_failure_with_log(
    current_span: Any,
    error: BaseException,
    *,
    error_code: str,
    retryable: bool = False,
    cancelled: bool = False,
    context: dict[str, Any] | None = None,
) -> None:
    import logging

    from .redaction import redact

    record_failure(
        current_span, error, error_code=error_code, retryable=retryable, cancelled=cancelled
    )
    if not cancelled:
        logging.getLogger("universal_runtime.runtime").error(
            "runtime execution failed",
            extra=redact({"error_code": error_code, **(context or {})}),
            exc_info=True,
        )
