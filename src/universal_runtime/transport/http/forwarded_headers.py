from __future__ import annotations

from collections.abc import Mapping

_FORWARDED = {
    "x-forwarded-host",
    "x-forwarded-proto",
    "x-forwarded-prefix",
    "x-forwarded-for",
    "forwarded",
    "traceparent",
    "tracestate",
    "x-request-id",
}


def trusted_forwarded_headers(headers: Mapping[str, str], *, trusted: bool) -> dict[str, str]:
    if not trusted:
        return {}
    return {key: value for key, value in headers.items() if key.lower() in _FORWARDED}
