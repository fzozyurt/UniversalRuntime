from __future__ import annotations

import os
import re
from collections.abc import Mapping
from typing import Any

_EXPRESSION = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::([-?])(.*))?\}")
_SECRET_NAME = re.compile(r"(key|token|secret|password|credential)", re.IGNORECASE)


def interpolate_environment(value: Any, environ: Mapping[str, str] | None = None) -> Any:
    """Resolve the three supported `${VAR}` interpolation forms recursively."""
    variables = os.environ if environ is None else environ
    if isinstance(value, Mapping):
        return {key: interpolate_environment(item, variables) for key, item in value.items()}
    if isinstance(value, list):
        return [interpolate_environment(item, variables) for item in value]
    if not isinstance(value, str):
        return value

    def replace(match: re.Match[str]) -> str:
        name, operator, argument = match.groups()
        if name in variables and variables[name] != "":
            return variables[name]
        if operator == "-":
            return argument or ""
        if operator == "?":
            raise ValueError(argument or f"environment variable {name} is required")
        if name not in variables:
            raise ValueError(f"environment variable {name} is required")
        return variables[name]

    result = _EXPRESSION.sub(replace, value)
    if "${" in result:
        raise ValueError("unsupported environment interpolation expression")
    return result


def redact_secrets(value: Any, *, replacement: str = "[REDACTED]") -> Any:
    """Return a copy with secret-like mapping keys redacted."""
    if isinstance(value, Mapping):
        return {
            key: replacement
            if _SECRET_NAME.search(str(key))
            else redact_secrets(item, replacement=replacement)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact_secrets(item, replacement=replacement) for item in value]
    return value
