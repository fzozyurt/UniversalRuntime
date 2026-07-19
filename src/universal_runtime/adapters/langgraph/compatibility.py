from __future__ import annotations

from typing import Any


def is_supported_graph_object(value: Any) -> bool:
    return hasattr(value, "ainvoke") and hasattr(value, "astream")
