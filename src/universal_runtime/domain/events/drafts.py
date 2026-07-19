from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass

from universal_runtime.domain.events.types import RuntimeEventType
from universal_runtime.domain.identity import ExecutionIdentity
from universal_runtime.domain.primitives.json_types import JsonObject, JsonValue


@dataclass(frozen=True, slots=True)
class RuntimeEventDraft:
    identity: ExecutionIdentity
    type: RuntimeEventType | str
    namespace: tuple[str, ...] = ()
    data: JsonValue = None
    native: JsonObject = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        object.__setattr__(self, "native", deepcopy(self.native or {}))
        object.__setattr__(self, "data", deepcopy(self.data))
