from __future__ import annotations

from typing import Any

from .noop import NoopMeter


def runtime_metrics(meter: Any | None = None) -> dict[str, Any]:
    meter = meter or NoopMeter()
    return {
        "http_requests": meter.create_counter("runtime.http.requests", unit="{request}"),
        "http_latency": meter.create_histogram("runtime.http.duration", unit="s"),
        "runs": meter.create_counter("runtime.runs", unit="{run}"),
        "run_duration": meter.create_histogram("runtime.run.duration", unit="s"),
        "active_executions": meter.create_up_down_counter("runtime.worker.active_executions"),
        "stream_events": meter.create_counter("runtime.stream.events", unit="{event}"),
        "queue_pending": meter.create_up_down_counter("runtime.queue.pending"),
    }
