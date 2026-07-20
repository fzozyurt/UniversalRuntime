from __future__ import annotations

import logging
import os
import threading

_LOCK = threading.Lock()
_CONFIGURED = False


def configure_logging() -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return
    with _LOCK:
        if _CONFIGURED:
            return
        level_name = os.environ.get("UR_LOG_LEVEL", "INFO").upper()
        level = getattr(logging, level_name, logging.INFO)
        logging.basicConfig(
            level=level,
            format=(
                "%(asctime)s %(levelname)s %(name)s "
                "runtime_instance=%(process)d %(message)s"
            ),
        )
        _CONFIGURED = True
