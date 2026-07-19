from __future__ import annotations

from copy import deepcopy

from universal_runtime.domain.primitives.json_types import JsonObject, JsonValue


def merge_config_precedence(*layers: JsonObject) -> JsonObject:
    """Merge config layers from lowest to highest precedence."""

    result: JsonObject = {}
    for layer in layers:
        _merge_object(result, layer)
    return result


def _merge_object(target: JsonObject, source: JsonObject) -> None:
    for key, value in source.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            _merge_object(target[key], value)  # type: ignore[arg-type]
        else:
            target[key] = deepcopy(value)


def copy_json(value: JsonValue) -> JsonValue:
    return deepcopy(value)
