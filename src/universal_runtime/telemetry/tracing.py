from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any


@contextmanager
def runtime_run_span(
    tracer: Any,
    attributes: dict[str, object],
) -> Iterator[Any]:
    with tracer.start_as_current_span(
        "runtime.run",
        attributes=attributes,
    ) as span:
        yield span


def record_failure(
    span: Any,
    exception: BaseException,
    *,
    error_code: str,
) -> None:
    record_exception = getattr(span, "record_exception", None)
    if callable(record_exception):
        record_exception(exception)
    set_attribute = getattr(span, "set_attribute", None)
    if callable(set_attribute):
        set_attribute("error.type", type(exception).__name__)
        set_attribute("error.message", str(exception))
        set_attribute("runtime.error_code", error_code)
