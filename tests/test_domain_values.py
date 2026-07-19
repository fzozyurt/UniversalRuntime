from datetime import UTC, datetime

import pytest

from universal_runtime.domain.capabilities import AdapterCapabilities
from universal_runtime.domain.events import RuntimeEvent
from universal_runtime.domain.execution import ExecutionRequest, QueuePriority
from universal_runtime.domain.identity import (
    ApplicationId,
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
        workspace_id=WorkspaceId("workspace"),
        project_id=ProjectId("project"),
        application_id=ApplicationId("application"),
        revision_id=RevisionId("revision"),
        deployment_id=DeploymentId("deployment"),
        assistant_id=AssistantId("assistant"),
        thread_id=ThreadId("thread"),
        run_id=RunId("run"),
        attempt_id=AttemptId("attempt"),
    )


def test_execution_request_has_interactive_defaults() -> None:
    request = ExecutionRequest(identity=identity(), assistant_id="assistant")
    assert request.priority is QueuePriority.INTERACTIVE
    assert request.stream_modes == ("values",)
    assert request.timeout_seconds == 1800


def test_domain_values_are_immutable() -> None:
    with pytest.raises(AttributeError):
        identity().run_id = RunId("other")  # type: ignore[misc]

    capabilities = AdapterCapabilities()
    assert capabilities.streaming is True


def test_runtime_event_preserves_identity_and_native_payload() -> None:
    event = RuntimeEvent(
        event_id="event",
        sequence=0,
        timestamp=datetime.now(UTC),
        identity=identity(),
        type="run.started",
        data={"ok": True},
        native={"source": "bootstrap"},
    )
    assert event.identity.run_id == "run"
    assert event.native["source"] == "bootstrap"
