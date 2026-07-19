from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from google.protobuf.struct_pb2 import ListValue, Struct, Value


def value_to_python(value: Value | None) -> Any:
    if value is None:
        return None
    kind = value.WhichOneof("kind")
    if kind == "null_value":
        return None
    if kind == "number_value":
        return value.number_value
    if kind == "string_value":
        return value.string_value
    if kind == "bool_value":
        return value.bool_value
    if kind == "struct_value":
        return {key: value_to_python(item) for key, item in value.struct_value.fields.items()}
    if kind == "list_value":
        return [value_to_python(item) for item in value.list_value.values]
    return None


def python_to_value(payload: Any) -> Value:
    result = Value()
    if payload is None:
        result.null_value = 0
    elif isinstance(payload, bool):
        result.bool_value = payload
    elif isinstance(payload, (int, float)):
        result.number_value = payload
    elif isinstance(payload, str):
        result.string_value = payload
    elif isinstance(payload, Mapping):
        result.struct_value.CopyFrom(
            Struct(fields={key: python_to_value(item) for key, item in payload.items()})
        )
    elif isinstance(payload, (list, tuple)):
        result.list_value.CopyFrom(ListValue(values=[python_to_value(item) for item in payload]))
    else:
        raise TypeError(f"unsupported protobuf Value payload: {type(payload).__name__}")
    return result
