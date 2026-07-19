from __future__ import annotations

from collections.abc import Iterable, MutableMapping
from typing import Any

_TRACE_HEADERS = ("traceparent", "tracestate", "baggage")


def _carrier(headers: Iterable[tuple[Any, Any]] | MutableMapping[str, Any]) -> dict[str, str]:
    if hasattr(headers, "items"):
        return {str(k).lower(): str(v) for k, v in headers.items()}  # type: ignore[union-attr]
    return {str(k).lower(): (v.decode() if isinstance(v, bytes) else str(v)) for k, v in headers}


def extract(headers: Iterable[tuple[Any, Any]] | MutableMapping[str, Any]) -> dict[str, str]:
    values = _carrier(headers)
    result = {key: values[key] for key in _TRACE_HEADERS if key in values}
    traceparent = result.get("traceparent", "")
    if traceparent and (len(traceparent.split("-")) != 4 or len(traceparent.split("-")[1]) != 32):
        return {}
    return result


def inject(carrier: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
    try:
        from opentelemetry.propagate import inject as otel_inject
    except ImportError:
        return carrier
    otel_inject(carrier)
    return carrier


def kafka_headers(headers: Iterable[tuple[str, bytes]]) -> dict[str, str]:
    return extract(headers)
