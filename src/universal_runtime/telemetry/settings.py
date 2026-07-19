from __future__ import annotations

import os
from dataclasses import dataclass
from enum import StrEnum


class ContentCapture(StrEnum):
    NONE = "none"
    METADATA = "metadata"
    REDACTED = "redacted"
    FULL = "full"


def _bool(value: str | None, default: bool = False) -> bool:
    return default if value is None else value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True, slots=True)
class TelemetrySettings:
    enabled: bool = False
    service_name: str = "universal-runtime"
    service_version: str = "0.1.0"
    otlp_endpoint: str | None = None
    otlp_protocol: str = "grpc"
    otlp_headers: tuple[tuple[str, str], ...] = ()
    traces_exporter: str = "otlp"
    metrics_exporter: str = "otlp"
    logs_exporter: str = "none"
    openlit: bool = False
    content_capture: ContentCapture = ContentCapture.METADATA
    shutdown_timeout_seconds: float = 5.0

    @classmethod
    def from_environment(cls, environ: dict[str, str] | None = None) -> TelemetrySettings:
        values = environ if environ is not None else os.environ
        raw_capture = values.get("UR_OBSERVABILITY_CONTENT_CAPTURE", "metadata").lower()
        try:
            capture = ContentCapture(raw_capture)
        except ValueError as exc:
            raise ValueError(
                "UR_OBSERVABILITY_CONTENT_CAPTURE must be none, metadata, redacted or full"
            ) from exc
        headers = tuple(
            (key.strip(), value.strip())
            for item in values.get("OTEL_EXPORTER_OTLP_HEADERS", "").split(",")
            if item.strip() and "=" in item
            for key, value in [item.split("=", 1)]
            if key.strip()
        )
        return cls(
            enabled=_bool(values.get("UR_OBSERVABILITY_ENABLED")),
            service_name=values.get("OTEL_SERVICE_NAME", "universal-runtime"),
            service_version=values.get("OTEL_SERVICE_VERSION", "0.1.0"),
            otlp_endpoint=values.get("OTEL_EXPORTER_OTLP_ENDPOINT"),
            otlp_protocol=values.get("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc").lower(),
            otlp_headers=headers,
            traces_exporter=values.get("OTEL_TRACES_EXPORTER", "otlp"),
            metrics_exporter=values.get("OTEL_METRICS_EXPORTER", "otlp"),
            logs_exporter=values.get("OTEL_LOGS_EXPORTER", "none"),
            openlit=_bool(values.get("OPENLIT_ENABLED", values.get("UR_OPENLIT_ENABLED"))),
            content_capture=capture,
            shutdown_timeout_seconds=float(values.get("UR_OBSERVABILITY_SHUTDOWN_TIMEOUT", "5")),
        )

    def validate(self) -> None:
        if self.otlp_protocol not in {"grpc", "http/protobuf", "http/json"}:
            raise ValueError("OTEL_EXPORTER_OTLP_PROTOCOL is unsupported")
        if self.shutdown_timeout_seconds < 0:
            raise ValueError("UR_OBSERVABILITY_SHUTDOWN_TIMEOUT must not be negative")
