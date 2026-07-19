from __future__ import annotations

from dataclasses import dataclass

from universal_runtime.domain.primitives.json_types import JsonObject


@dataclass(frozen=True, slots=True)
class RunError:
    code: str
    message: str
    retryable: bool = False
    details: JsonObject = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        object.__setattr__(self, "details", {} if self.details is None else _copy(self.details))


def _copy(value: JsonObject) -> JsonObject:
    return {
        key: _copy(item)
        if isinstance(item, dict)
        else [_copy(x) if isinstance(x, dict) else x for x in item]
        if isinstance(item, list)
        else item
        for key, item in value.items()
    }
