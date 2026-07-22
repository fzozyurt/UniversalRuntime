import json
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError as SchemaValidationError
from pydantic import ValidationError

from universal_runtime.adapters.kafka.topics import TopicNames
from universal_runtime.configuration.interpolation import interpolate_environment, redact_secrets
from universal_runtime.transport.http.dto import Assistant, RunCreate

ROOT = Path(__file__).resolve().parents[2]


def test_openapi_dto_shapes_match_contract_examples() -> None:
    assistant = Assistant(assistant_id="assistant", graph_id="graph", version=1)
    run = RunCreate(assistant_id="assistant", input=["message"], stream_mode=["values", "messages"])
    assert assistant.model_dump()["version"] == 1
    assert run.stream_mode == ["values", "messages"]
    with pytest.raises(ValidationError):
        RunCreate.model_validate({})


def test_config_schema_accepts_example_and_rejects_unknown_fields() -> None:
    schema = json.loads((ROOT / "contracts/config/runtime-application.schema.json").read_text())
    example = yaml.safe_load((ROOT / "deployment" / "runtime.example.yaml").read_text())
    Draft202012Validator(schema).validate(example)
    invalid = {**example, "unexpected": True}
    with pytest.raises(SchemaValidationError):
        Draft202012Validator(schema).validate(invalid)


def test_event_schema_accepts_namespace_and_rejects_invalid_type() -> None:
    schema = json.loads((ROOT / "contracts/events/runtime-event-v1.schema.json").read_text())
    validator = Draft202012Validator(schema)
    event = {
        "schema_version": 1,
        "event_id": "event",
        "sequence": 0,
        "timestamp": "2026-01-01T00:00:00Z",
        "application_id": "application",
        "run_id": "run",
        "type": "tool.started",
        "namespace": ["supervisor", "agent"],
        "data": {"name": "search"},
    }
    validator.validate(event)
    event["type"] = "ToolStarted"
    with pytest.raises(SchemaValidationError):
        validator.validate(event)


def test_topic_prefix_and_individual_overrides_are_deterministic() -> None:
    topics = TopicNames.from_config(prefix="custom", environment="prod")
    assert topics.short_queue == "custom.prod.runs.short_queue"
    assert topics.commands == "custom.prod.run.commands"
    overridden = TopicNames.from_config(
        prefix="custom",
        environment="prod",
        overrides={"long_queue": "priority.long_queue"},
    )
    assert overridden.long_queue == "priority.long_queue"
    assert overridden.short_queue == topics.short_queue
    with pytest.raises(ValueError, match="unknown topic"):
        TopicNames.from_config(overrides={"unknown": "topic"})


def test_environment_interpolation_supports_contract_forms_and_rejects_expressions() -> None:
    environment = {"MODEL_NAME": "gpt-test", "EMPTY": ""}
    assert interpolate_environment("${MODEL_NAME}", environment) == "gpt-test"
    assert interpolate_environment("${MISSING:-fallback}", environment) == "fallback"
    with pytest.raises(ValueError, match="required"):
        interpolate_environment("${MISSING:?DATABASE_URL is required}", environment)
    with pytest.raises(ValueError, match="unsupported"):
        interpolate_environment("${MODEL_NAME|shell_command}", environment)

    config = {"OPENAI_API_KEY": "secret", "nested": {"password": "value", "name": "safe"}}
    assert redact_secrets(config) == {
        "OPENAI_API_KEY": "[REDACTED]",
        "nested": {"password": "[REDACTED]", "name": "safe"},
    }
