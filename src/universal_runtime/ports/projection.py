from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from universal_runtime.domain.primitives.json_types import JsonObject, JsonValue


@dataclass(frozen=True, slots=True)
class RunProjection:
    run_id: str
    status: str
    sequence: int
    started_at: str | None = None
    completed_at: str | None = None
    result: JsonValue = None
    error: JsonObject | None = None
    metadata: JsonObject = field(default_factory=dict)


class ProjectionSink(Protocol):
    async def write(self, projection: RunProjection) -> None: ...
    async def close(self) -> None: ...


class ArtifactStore(Protocol):
    async def put(
        self, *, run_id: str, data: bytes, media_type: str, filename: str | None = None
    ) -> JsonObject: ...
    async def close(self) -> None: ...
