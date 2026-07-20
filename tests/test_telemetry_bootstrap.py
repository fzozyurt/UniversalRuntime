from __future__ import annotations

from universal_runtime.telemetry.bootstrap import initialize
from universal_runtime.telemetry.tracing import record_failure, runtime_run_span


def test_disabled_telemetry_produces_safe_noop_spans(monkeypatch) -> None:
    monkeypatch.setenv("UR_OTEL_ENABLED", "false")

    telemetry = initialize(component="worker")

    assert telemetry.enabled is False
    with runtime_run_span(
        telemetry.tracer,
        {"runtime.run_id": "run-1"},
    ) as span:
        record_failure(
            span,
            RuntimeError("boom"),
            error_code="TEST_FAILURE",
        )
