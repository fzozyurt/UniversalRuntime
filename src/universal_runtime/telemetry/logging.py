from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

from .redaction import redact


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        fields: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "message": record.getMessage().replace("\n", "\\n"),
            "service": "universal-runtime",
            "component": record.name,
        }
        fields.update(
            {
                key: getattr(record, key)
                for key in (
                    "request_id",
                    "run_id",
                    "thread_id",
                    "trace_id",
                    "span_id",
                    "error_code",
                )
                if hasattr(record, key)
            }
        )
        if record.exc_info:
            fields["exception.type"] = record.exc_info[0].__name__
        return json.dumps(redact(fields), default=str, separators=(",", ":"))


def configure_logging() -> None:
    root = logging.getLogger()
    if any(isinstance(handler.formatter, JsonFormatter) for handler in root.handlers):
        return
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)
    root.setLevel(logging.INFO)
