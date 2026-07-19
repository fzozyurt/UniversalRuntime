from __future__ import annotations

from typing import Any

from .redaction import redact
from .settings import ContentCapture


def capture_content(
    value: Any, policy: ContentCapture, *, max_bytes: int = 16_384
) -> dict[str, Any]:
    if policy is ContentCapture.NONE:
        return {}
    if policy is ContentCapture.METADATA:
        return {
            "type": type(value).__name__,
            "length": len(value) if hasattr(value, "__len__") else None,
        }
    captured = redact(value) if policy is ContentCapture.REDACTED else value
    text = repr(captured)
    return {"value": text[:max_bytes], "truncated": len(text) > max_bytes}
