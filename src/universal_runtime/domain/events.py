from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from universal_runtime.domain.identity import ExecutionIdentity


@dataclass(frozen=True, slots=True)
class RuntimeEvent:
    event_id: str
    sequence: int
    timestamp: datetime
    identity: ExecutionIdentity
    type: str
    namespace: tuple[str, ...] = ()
    data: Any = None
    trace: dict[str, str | None] = field(default_factory=dict)
    native: dict[str, Any] = field(default_factory=dict)
    schema_version: int = 1
