from __future__ import annotations

from copy import deepcopy

from universal_runtime.ports.projection import ProjectionSink, RunProjection


class NullProjectionSink(ProjectionSink):
    async def write(self, projection: RunProjection) -> None:
        del projection

    async def close(self) -> None:
        return None


class InMemoryProjectionSink(ProjectionSink):
    def __init__(self) -> None:
        self.projections: dict[str, RunProjection] = {}

    async def write(self, projection: RunProjection) -> None:
        self.projections[projection.run_id] = deepcopy(projection)

    async def close(self) -> None:
        return None


class CompositeProjectionSink(ProjectionSink):
    def __init__(self, *sinks: ProjectionSink) -> None:
        self._sinks = sinks

    async def write(self, projection: RunProjection) -> None:
        for sink in self._sinks:
            await sink.write(projection)

    async def close(self) -> None:
        for sink in self._sinks:
            await sink.close()
