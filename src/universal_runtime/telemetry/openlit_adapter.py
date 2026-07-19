from __future__ import annotations

import logging

_LOGGER = logging.getLogger(__name__)


def initialize_openlit(enabled: bool, *, content_capture: str) -> bool:
    if not enabled:
        return False
    try:
        import openlit
    except ImportError:
        _LOGGER.warning("OpenLIT requested but optional dependency is unavailable")
        return False
    try:
        # OpenLIT exports through the active OTel provider; no second provider is installed.
        openlit.init(disable_metrics=False, capture_content=content_capture != "none")
    except Exception:
        _LOGGER.exception("OpenLIT initialization failed; continuing with OTel")
        return False
    return True
