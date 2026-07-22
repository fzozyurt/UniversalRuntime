from __future__ import annotations

import os

import structlog

_initialized = False


def init_observability() -> None:
    global _initialized
    if _initialized:
        return
    _initialized = True

    from universal_runtime.telemetry.logging import configure_logging

    configure_logging()

    if os.environ.get("UR_OBSERVABILITY_ENABLED", "").lower() != "true":
        return

    logger = structlog.get_logger(__name__)

    try:
        import openlit

        openlit.init(
            service_name=os.environ.get(
                "OTEL_SERVICE_NAME",
                os.environ.get("UR_APPLICATION_ID", "universal-runtime"),
            ),
            environment=os.environ.get("OTEL_DEPLOYMENT_ENVIRONMENT", "default"),
            capture_message_content=False,
            disable_metrics=False,
            disable_events=False,
        )
        logger.info(
            "observability initialized",
            service=os.environ.get("OTEL_SERVICE_NAME", "universal-runtime"),
            otlp_endpoint=os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "none"),
        )
    except ImportError:
        logger.warning("openlit is not installed; observability disabled")
