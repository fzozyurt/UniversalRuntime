from __future__ import annotations

from contextlib import contextmanager
from typing import Any

from opentelemetry.trace import Span, Status, StatusCode


@contextmanager
def runtime_run_span(
    tracer: Any,
    attributes: dict[str, str] | None = None,
    span_name: str = "runtime.run",
) -> Any:
    with tracer.start_as_current_span(span_name) as span:
        if attributes:
            for key, value in attributes.items():
                span.set_attribute(key, value)
        yield span


def record_failure(
    span: Span,
    exception: Exception,
    error_code: str = "UNKNOWN",
) -> None:
    span.set_status(Status(StatusCode.ERROR, str(exception)))
    span.record_exception(exception)
    span.set_attribute("error.code", error_code)
