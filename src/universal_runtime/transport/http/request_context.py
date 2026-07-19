from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class HttpRequestContext:
    request_id: str
    application_id: str
    revision_id: str | None = None
    deployment_id: str | None = None
    traceparent: str | None = None
    tracestate: str | None = None
