from __future__ import annotations

from universal_runtime.telemetry.bootstrap import initialize, reset_for_tests
from universal_runtime.telemetry.content_policy import capture_content
from universal_runtime.telemetry.propagation import extract
from universal_runtime.telemetry.redaction import redact
from universal_runtime.telemetry.settings import ContentCapture, TelemetrySettings


def teardown_function() -> None:
    reset_for_tests()


def test_disabled_path_does_not_require_exporter() -> None:
    runtime = initialize(TelemetrySettings(enabled=False, otlp_endpoint=None))
    assert runtime.enabled is False
    assert runtime.tracer is not None


def test_settings_parse_secret_safe_headers_without_logging_them() -> None:
    settings = TelemetrySettings.from_environment(
        {
            "UR_OBSERVABILITY_ENABLED": "true",
            "OTEL_EXPORTER_OTLP_HEADERS": "authorization=secret,tenant=one",
        }
    )
    assert settings.otlp_headers == (("authorization", "secret"), ("tenant", "one"))


def test_nested_redaction_and_dsn_password() -> None:
    value = redact(
        {
            "headers": {"Authorization": "secret"},
            "items": [{"password": "x"}],
            "dsn": "postgres://u:p@db/x",
        }
    )
    assert value["headers"]["Authorization"] == "[REDACTED]"
    assert value["items"][0]["password"] == "[REDACTED]"  # noqa: S105
    assert "[REDACTED]" in value["dsn"]


def test_content_policy_metadata_and_none() -> None:
    assert capture_content("prompt", ContentCapture.NONE) == {}
    assert capture_content("prompt", ContentCapture.METADATA)["length"] == 6


def test_invalid_trace_headers_are_ignored() -> None:
    assert extract({"traceparent": "not-a-trace"}) == {}
