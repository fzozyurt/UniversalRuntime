from __future__ import annotations

import logging
import os

_LOGGER = logging.getLogger(__name__)
_initialized = False


def init_observability() -> None:
    global _initialized
    if _initialized:
        return
    _initialized = True
    if os.environ.get("UR_OBSERVABILITY_ENABLED", "").lower() != "true":
        return
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
        _LOGGER.info(
            "observability initialized (service=%s, endpoint=%s)",
            os.environ.get("OTEL_SERVICE_NAME", "universal-runtime"),
            os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "none"),
        )
    except ImportError:
        _LOGGER.warning("openlit is not installed; observability disabled")
