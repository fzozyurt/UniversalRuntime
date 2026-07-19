from __future__ import annotations


def deterministic_weather(city: str) -> str:
    """Return a deterministic tool result without a model or network call."""
    return f"weather:{city}:sunny"
