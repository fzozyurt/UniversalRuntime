from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime

from universal_runtime.domain.identity import AssistantId
from universal_runtime.domain.primitives.json_types import JsonObject


@dataclass(frozen=True, slots=True)
class Assistant:
    assistant_id: AssistantId
    graph_id: str
    version: int = 1
    name: str | None = None
    description: str | None = None
    config: JsonObject = field(default_factory=dict)
    context: JsonObject = field(default_factory=dict)
    metadata: JsonObject = field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None

    def __post_init__(self) -> None:
        for name in ("config", "context", "metadata"):
            object.__setattr__(self, name, deepcopy(getattr(self, name)))
