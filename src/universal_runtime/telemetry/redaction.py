from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

SENSITIVE_KEYS = frozenset(
    {
        "password",
        "secret",
        "token",
        "api_key",
        "authorization",
        "cookie",
        "set-cookie",
        "connection_string",
    }
)
_DSN_PASSWORD = re.compile(r"(://[^:/]+:)[^@]+(@)")


def redact(
    value: Any, *, max_depth: int = 8, max_items: int = 100, replacement: str = "[REDACTED]"
) -> Any:
    if max_depth < 0:
        return "[TRUNCATED]"
    if isinstance(value, Mapping):
        result: dict[Any, Any] = {}
        for index, (key, item) in enumerate(value.items()):
            if index >= max_items:
                result["[TRUNCATED]"] = True
                break
            result[key] = (
                replacement
                if str(key).lower().replace("-", "_") in SENSITIVE_KEYS
                else redact(
                    item, max_depth=max_depth - 1, max_items=max_items, replacement=replacement
                )
            )
        return result
    if isinstance(value, (list, tuple)):
        return [
            redact(item, max_depth=max_depth - 1, max_items=max_items, replacement=replacement)
            for item in value[:max_items]
        ]
    if isinstance(value, bytes):
        return "[BINARY OMITTED]"
    if isinstance(value, str):
        return _DSN_PASSWORD.sub(r"\1[REDACTED]\2", value)
    return value
