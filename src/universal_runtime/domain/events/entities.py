from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime

from universal_runtime.domain.events.types import RuntimeEventType
from universal_runtime.domain.identity import EventId, ExecutionIdentity
from universal_runtime.domain.primitives.json_types import JsonObject, JsonValue


@dataclass(frozen=True, slots=True)
class TraceContext:
    trace_id: str | None = None
    span_id: str | None = None


@dataclass(frozen=True, slots=True)
class RuntimeEvent:
    event_id: EventId
    sequence: int
    timestamp: datetime
    identity: ExecutionIdentity
    type: RuntimeEventType | str
    namespace: tuple[str, ...] = ()
    data: JsonValue = None
    native: JsonObject = field(default_factory=dict)
    trace: TraceContext = field(default_factory=TraceContext)
    schema_version: int = 1

    def __post_init__(self) -> None:
        object.__setattr__(self, "data", deepcopy(self.data))
        object.__setattr__(self, "native", deepcopy(self.native))
