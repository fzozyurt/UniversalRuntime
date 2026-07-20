from __future__ import annotations

import importlib
import os
import threading
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any

_TRUE_VALUES = {"1", "true", "yes", "on"}
_PROVIDER_LOCK = threading.Lock()
_PROVIDER_CONFIGURED = False


class NoopSpan:
    def set_attribute(self, name: str, value: object) -> None:
        del name, value

    def record_exception(self, exception: BaseException) -> None:
        del exception


class NoopTracer:
    def start_as_current_span(
        self,
        name: str,
        *,
        attributes: dict[str, object] | None = None,
    ) -> Any:
        del name, attributes
        return nullcontext(NoopSpan())


@dataclass(frozen=True, slots=True)
class TelemetryRuntime:
    tracer: Any
    enabled: bool
    component: str


def _enabled() -> bool:
    raw = os.environ.get("UR_OTEL_ENABLED")
    if raw is None:
        raw = os.environ.get("OTEL_SDK_DISABLED", "false")
        return raw.strip().lower() not in _TRUE_VALUES
    return raw.strip().lower() in _TRUE_VALUES


def _configure_provider(component: str) -> Any:
    global _PROVIDER_CONFIGURED

    trace = importlib.import_module("opentelemetry.trace")
    with _PROVIDER_LOCK:
        if not _PROVIDER_CONFIGURED:
            try:
                resources = importlib.import_module("opentelemetry.sdk.resources")
                sdk_trace = importlib.import_module("opentelemetry.sdk.trace")
                export = importlib.import_module("opentelemetry.sdk.trace.export")
                provider = sdk_trace.TracerProvider(
                    resource=resources.Resource.create(
                        {
                            "service.name": os.environ.get(
                                "OTEL_SERVICE_NAME",
                                f"universal-runtime-{component}",
                            ),
                            "service.namespace": os.environ.get(
                                "OTEL_SERVICE_NAMESPACE",
                                "universal-runtime",
                            ),
                            "runtime.component": component,
                        }
                    )
                )
                endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
                if endpoint:
                    protocol = os.environ.get(
                        "OTEL_EXPORTER_OTLP_PROTOCOL",
                        "grpc",
                    ).lower()
                    module_name = (
                        "opentelemetry.exporter.otlp.proto.http.trace_exporter"
                        if protocol.startswith("http")
                        else "opentelemetry.exporter.otlp.proto.grpc.trace_exporter"
                    )
                    exporter_module = importlib.import_module(module_name)
                    exporter = exporter_module.OTLPSpanExporter(endpoint=endpoint)
                    provider.add_span_processor(export.BatchSpanProcessor(exporter))
                trace.set_tracer_provider(provider)
                _PROVIDER_CONFIGURED = True
            except (ImportError, ModuleNotFoundError):
                # The API or a vendor agent may still provide the global tracer.
                # Base installations intentionally do not require the SDK/exporter.
                _PROVIDER_CONFIGURED = True
    return trace.get_tracer(
        "universal_runtime",
        os.environ.get("UR_RUNTIME_VERSION", "0.1.0"),
    )


def initialize(*, component: str) -> TelemetryRuntime:
    if not _enabled():
        return TelemetryRuntime(NoopTracer(), False, component)
    try:
        tracer = _configure_provider(component)
    except (ImportError, ModuleNotFoundError):
        return TelemetryRuntime(NoopTracer(), False, component)
    return TelemetryRuntime(tracer, True, component)
