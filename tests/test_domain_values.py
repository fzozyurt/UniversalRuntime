from datetime import UTC, datetime

import pytest

from universal_runtime.domain.capabilities import AdapterCapabilities
from universal_runtime.domain.events import RuntimeEvent
from universal_runtime.domain.execution import ExecutionRequest, QueuePriority
from universal_runtime.domain.identity import (
    ApplicationId,
    ApplicationScope,
    AssistantId,
    AttemptId,
    DeploymentId,
    ExecutionIdentity,
    ProjectId,
    RevisionId,
    RunId,
    ThreadId,
    WorkspaceId,
)


def identity() -> ExecutionIdentity:
    return ExecutionIdentity(
        ApplicationScope(
            WorkspaceId.parse("workspace"),
            ProjectId.parse("project"),
            ApplicationId.parse("application"),
            RevisionId.parse("revision"),
            DeploymentId.parse("deployment"),
        ),
        AssistantId.parse("assistant"),
        RunId.parse("run"),
        AttemptId.parse("attempt"),
        ThreadId.parse("thread"),
    )


def test_execution_request_has_interactive_defaults() -> None:
    request = ExecutionRequest(identity=identity())
    assert request.priority is QueuePriority.INTERACTIVE
    assert request.stream_modes == ("values",)
    assert request.timeout_seconds == 1800


def test_domain_values_are_immutable() -> None:
    with pytest.raises(AttributeError):
        identity().run_id = RunId.parse("other")  # type: ignore[misc]
    capabilities = AdapterCapabilities()
    assert capabilities.streaming is True


def test_runtime_event_preserves_identity_and_native_payload() -> None:
    event = RuntimeEvent(
        event_id=__import__(
            "universal_runtime.domain.identity", fromlist=["EventId"]
        ).EventId.parse("event"),
        sequence=0,
        timestamp=datetime.now(UTC),
        identity=identity(),
        type="run.started",
        data={"ok": True},
        native={"source": "bootstrap"},
        trace=__import__(
            "universal_runtime.domain.events", fromlist=["TraceContext"]
        ).TraceContext(),
    )
    assert event.identity.run_id == "run"
    assert event.native["source"] == "bootstrap"
