from __future__ import annotations

from universal_runtime.domain.events import RuntimeEvent, RuntimeEventType
from universal_runtime.ports.projection import ProjectionSink, RunProjection

_TERMINAL = {
    RuntimeEventType.RUN_COMPLETED,
    RuntimeEventType.RUN_CANCELLED,
    RuntimeEventType.RUN_FAILED,
    RuntimeEventType.RUN_TIMEOUT,
}


class LifecycleProjector:
    def __init__(self, sink: ProjectionSink, *, consumer_name: str = "runtime-projector") -> None:
        self._sink = sink
        self._consumer_name = consumer_name
        self._last_sequence: dict[str, int] = {}
        self._inbox: set[tuple[str, str]] = set()
        self._projections: dict[str, RunProjection] = {}

    async def handle(self, event: RuntimeEvent) -> bool:
        key = str(event.identity.run_id)
        inbox_key = (self._consumer_name, str(event.event_id))
        if inbox_key in self._inbox:
            return False
        expected = self._last_sequence.get(key, -1) + 1
        if event.sequence != expected:
            raise ValueError(
                f"out-of-order event for {key}: expected {expected}, got {event.sequence}"
            )
        current = self._projections.get(
            key, RunProjection(run_id=key, status="pending", sequence=-1)
        )
        status = current.status
        started_at = current.started_at
        completed_at = current.completed_at
        result = current.result
        error = current.error
        if event.type is RuntimeEventType.RUN_STARTED:
            status, started_at = "running", event.timestamp.isoformat()
        elif event.type is RuntimeEventType.RUN_COMPLETED:
            status, completed_at, result = "success", event.timestamp.isoformat(), event.data
        elif event.type is RuntimeEventType.RUN_CANCELLED:
            status, completed_at = "cancelled", event.timestamp.isoformat()
        elif event.type is RuntimeEventType.RUN_FAILED:
            status, completed_at = "error", event.timestamp.isoformat()
            error = event.data if isinstance(event.data, dict) else {"message": str(event.data)}
        elif event.type is RuntimeEventType.RUN_TIMEOUT:
            status, completed_at = "timeout", event.timestamp.isoformat()
        projection = RunProjection(
            key, status, event.sequence, started_at, completed_at, result, error
        )
        self._inbox.add(inbox_key)
        self._last_sequence[key] = event.sequence
        self._projections[key] = projection
        await self._sink.write(projection)
        return True

    async def close(self) -> None:
        await self._sink.close()
