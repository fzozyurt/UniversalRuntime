from __future__ import annotations

from typing import Any

from universal_runtime.telemetry import init_observability


def initialize(component: str = "default") -> Any:
    init_observability()
    import structlog

    logger = structlog.get_logger(f"universal-runtime.{component}")
    logger.info("component started", component=component)

    from opentelemetry import trace

    tracer = trace.get_tracer(f"universal-runtime.{component}")
    return type("Telemetry", (), {"tracer": tracer, "logger": logger})()
