from __future__ import annotations

from datetime import UTC, datetime

import pytest

from universal_runtime.adapters.a2a.agent_card import build_agent_card
from universal_runtime.adapters.a2a.descriptor import descriptor_from_manifest
from universal_runtime.adapters.a2a.event_mapper import text_message
from universal_runtime.adapters.a2a.request_mapper import (
    context_thread_id,
    execution_request,
    message_input,
)
from universal_runtime.adapters.a2a.server import create_a2a_routes
from universal_runtime.adapters.a2a.status_mapper import task_state
from universal_runtime.bootstrap.local import create_local_runtime
from universal_runtime.domain.assistants import Assistant
from universal_runtime.domain.capabilities import (
    AdapterCapabilities,
    AdapterManifest,
    RuntimeProfile,
    StreamMode,
)
from universal_runtime.domain.errors import RuntimeFailure
from universal_runtime.domain.events import RuntimeEvent, RuntimeEventType, TraceContext
from universal_runtime.domain.identity import (
    ApplicationId,
    ApplicationScope,
    AssistantId,
    AttemptId,
    DeploymentId,
    EventId,
    ExecutionIdentity,
    ProjectId,
    RevisionId,
    RunId,
    ThreadId,
    WorkspaceId,
)


def _manifest() -> AdapterManifest:
    return AdapterManifest(
        adapter_id="langgraph",
        adapter_version="1",
        profiles=frozenset({RuntimeProfile.LANGGRAPH}),
        stream_modes=frozenset({StreamMode.EVENTS}),
        capabilities=AdapterCapabilities(streaming=True, a2a=True),
    )


def test_agent_card_advertises_only_enabled_capabilities() -> None:
    descriptor = descriptor_from_manifest(
        name="assistant",
        description="safe",
        version="1",
        url="https://runtime.example",
        manifest=_manifest(),
    )
    card = build_agent_card(descriptor)
    assert card.name == "assistant"
    assert card.capabilities.streaming is True
    assert [skill.id for skill in card.skills] == ["runtime.execute", "runtime.stream"]


def test_context_id_is_validated_without_hidden_mapping() -> None:
    assert str(context_thread_id("thread-1")) == "thread-1"
    with pytest.raises(RuntimeFailure):
        context_thread_id("")


def test_status_mapping_uses_official_task_states() -> None:
    from a2a.types import TaskState

    assert task_state("run.completed") == TaskState.TASK_STATE_COMPLETED
    assert task_state("run.unknown") is None


def test_message_text_and_json_parts_roundtrip() -> None:
    from a2a.types import Message, Part
    from google.protobuf.struct_pb2 import Struct, Value

    assert message_input(Message(parts=[Part(text="hello")])) == "hello"
    value = Value(struct_value=Struct(fields={"value": Value(number_value=1)}))
    assert message_input(Message(parts=[Part(data=value)])) == {"value": 1.0}


def test_unsupported_a2a_parts_are_typed_errors() -> None:
    from a2a.types import Message, Part

    with pytest.raises(RuntimeFailure):
        message_input(Message(parts=[Part(url="https://example.invalid/file")]))


def test_execution_request_preserves_context_and_identity() -> None:
    from a2a.types import Message, Part

    request = execution_request(
        message=Message(context_id="thread-1", message_id="message-1", parts=[Part(text="hi")]),
        assistant_id=AssistantId.parse("assistant"),
        run_id=RunId.parse("run"),
        scope=ApplicationScope(
            WorkspaceId.parse("workspace"),
            ProjectId.parse("project"),
            ApplicationId.parse("application"),
            RevisionId.parse("revision"),
            DeploymentId.parse("deployment"),
        ),
    )
    assert request.identity.thread_id == ThreadId.parse("thread-1")
    assert request.metadata["a2a.message_id"] == "message-1"


def test_runtime_message_event_maps_to_official_message() -> None:
    event = RuntimeEvent(
        event_id=EventId.parse("event"),
        sequence=1,
        timestamp=datetime.now(UTC),
        identity=ExecutionIdentity(
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
        ),
        type=RuntimeEventType.MESSAGE_DELTA,
        data="hello",
        trace=TraceContext(),
    )
    message = text_message(event)
    assert message is not None
    assert message.parts[0].text == "hello"


def test_non_message_event_is_not_emitted_as_message() -> None:
    assert (
        text_message(
            RuntimeEvent(
                event_id=EventId.parse("event"),
                sequence=1,
                timestamp=datetime.now(UTC),
                identity=ExecutionIdentity(
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
                ),
                type=RuntimeEventType.RUN_STARTED,
            )
        )
        is None
    )


@pytest.mark.asyncio
async def test_official_agent_card_route_uses_explicit_assistant() -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    runtime = create_local_runtime()
    assistant = await runtime.assistants.create(
        Assistant(assistant_id=AssistantId.new(), graph_id="graph", name="Demo")
    )
    app = FastAPI()
    app.routes.extend(
        create_a2a_routes(
            runtime=runtime,
            assistant=assistant,
            manifest=_manifest(),
            public_url="https://runtime.example/a2a",
        )
    )
    response = TestClient(app).get("/.well-known/agent-card.json")
    assert response.status_code == 200
    assert response.json()["name"] == "Demo"
