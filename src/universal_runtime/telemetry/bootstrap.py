from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .metrics import runtime_metrics
from .noop import NoopMeter, NoopTracer
from .openlit_adapter import initialize_openlit
from .resources import resource_attributes
from .settings import TelemetrySettings

_LOGGER = logging.getLogger(__name__)
_STATE: TelemetryRuntime | None = None


def _is_sdk_provider(provider: Any) -> bool:
    return provider.__class__.__module__.startswith("opentelemetry.sdk.")


@dataclass(slots=True)
class TelemetryRuntime:
    settings: TelemetrySettings
    tracer: Any
    meter: Any
    metrics: dict[str, Any]
    enabled: bool

    def shutdown(self) -> None:
        if not self.enabled:
            return
        try:
            from opentelemetry import metrics, trace

            tracer_provider = trace.get_tracer_provider()
            meter_provider = metrics.get_meter_provider()
            if hasattr(tracer_provider, "shutdown"):
                tracer_provider.shutdown()
            if hasattr(meter_provider, "shutdown"):
                meter_provider.shutdown()
        except (ImportError, RuntimeError):
            _LOGGER.debug("telemetry shutdown completed without SDK provider", exc_info=True)


def initialize(
    settings: TelemetrySettings | None = None,
    *,
    component: str = "runtime",
    attributes: Mapping[str, str] | None = None,
) -> TelemetryRuntime:
    global _STATE
    if _STATE is not None:
        return _STATE
    settings = settings or TelemetrySettings.from_environment()
    settings.validate()
    if not settings.enabled:
        _STATE = TelemetryRuntime(settings, NoopTracer(), NoopMeter(), runtime_metrics(), False)
        return _STATE
    try:
        from opentelemetry import metrics, trace
        from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        resource = Resource.create(
            resource_attributes(
                service_name=settings.service_name,
                service_version=settings.service_version,
                component=component,
                extra=attributes,
            )
        )
        current_tracer_provider = trace.get_tracer_provider()
        provider = (
            current_tracer_provider
            if _is_sdk_provider(current_tracer_provider)
            else TracerProvider(resource=resource)
        )
        if (
            provider is not current_tracer_provider
            and settings.traces_exporter.lower() != "none"
            and settings.otlp_endpoint
        ):
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

            provider.add_span_processor(
                BatchSpanProcessor(
                    OTLPSpanExporter(
                        endpoint=settings.otlp_endpoint, headers=dict(settings.otlp_headers)
                    )
                )
            )
        if provider is not current_tracer_provider:
            trace.set_tracer_provider(provider)
        readers = (
            []
            if settings.metrics_exporter.lower() == "none" or not settings.otlp_endpoint
            else [
                PeriodicExportingMetricReader(
                    OTLPMetricExporter(
                        endpoint=settings.otlp_endpoint, headers=dict(settings.otlp_headers)
                    )
                )
            ]
        )
        current_meter_provider = metrics.get_meter_provider()
        meter_provider = (
            current_meter_provider
            if _is_sdk_provider(current_meter_provider)
            else MeterProvider(resource=resource, metric_readers=readers)
        )
        if meter_provider is not current_meter_provider:
            metrics.set_meter_provider(meter_provider)
        runtime = TelemetryRuntime(
            settings,
            trace.get_tracer("universal-runtime"),
            meter_provider.get_meter("universal-runtime"),
            {},
            True,
        )
        runtime.metrics = runtime_metrics(runtime.meter)
        initialize_openlit(settings.openlit, content_capture=settings.content_capture.value)
        _STATE = runtime
        return runtime
    except ImportError:
        _LOGGER.warning(
            "OTel enabled but optional SDK/exporter dependencies are unavailable; using no-op telemetry"
        )
        _STATE = TelemetryRuntime(settings, NoopTracer(), NoopMeter(), runtime_metrics(), False)
        return _STATE


def instrument_optional_clients() -> tuple[str, ...]:
    installed: list[str] = []
    for module_name, class_name, label in (
        ("opentelemetry.instrumentation.httpx", "HTTPXClientInstrumentor", "httpx"),
        ("opentelemetry.instrumentation.sqlalchemy", "SQLAlchemyInstrumentor", "sqlalchemy"),
        ("opentelemetry.instrumentation.grpc", "GrpcInstrumentorClient", "grpc"),
        ("opentelemetry.instrumentation.langchain", "LangchainInstrumentor", "langchain"),
    ):
        try:
            module = __import__(module_name, fromlist=[class_name])
            getattr(module, class_name)().instrument()
        except (ImportError, AttributeError, RuntimeError):
            continue
        installed.append(label)
    return tuple(installed)


def instrument_fastapi(app: Any) -> bool:
    try:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        FastAPIInstrumentor.instrument_app(app)
    except (ImportError, RuntimeError):
        return False
    return True


def reset_for_tests() -> None:
    global _STATE
    _STATE = None
