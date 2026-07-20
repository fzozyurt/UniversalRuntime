from datetime import UTC, datetime

import pytest

from universal_runtime.domain.errors import RuntimeFailure
from universal_runtime.domain.execution import (
    ExecutionRequest,
    ExecutionTarget,
    QueuePriority,
    RunCommand,
)
from universal_runtime.domain.identity import (
    ApplicationId,
    ApplicationScope,
    AssistantId,
    AttemptId,
    CommandId,
    DeploymentId,
    ExecutionIdentity,
    ProjectId,
    RevisionId,
    RunId,
    ThreadId,
    WorkspaceId,
)
from universal_runtime.transport.queue_codec import (
    run_command_from_document,
    run_command_to_document,
)


def test_run_command_codec_preserves_scope_target_and_execution_options() -> None:
    identity = ExecutionIdentity(
        ApplicationScope(
            WorkspaceId.parse("workspace"),
            ProjectId.parse("project"),
            ApplicationId.parse("application"),
            RevisionId.parse("revision-7"),
            DeploymentId.parse("deployment-blue"),
        ),
        AssistantId.parse("assistant-vip"),
        RunId.parse("run-1"),
        AttemptId.parse("attempt-1"),
        ThreadId.parse("thread-1"),
    )
    request = ExecutionRequest(
        identity=identity,
        input={"messages": [{"role": "user", "content": "hello"}]},
        command={"resume": True},
        config={"configurable": {"tenant": "acme"}},
        context={"user_id": "u-1"},
        metadata={"source": "chat"},
        stream_modes=("messages", "updates", "custom"),
        stream_subgraphs=True,
        priority=QueuePriority.NORMAL,
        timeout_seconds=321,
        checkpoint_namespace="supervisor/research",
        checkpoint_id="checkpoint-9",
        target=ExecutionTarget("research-graph", 4),
    )
    timestamp = datetime(2026, 7, 20, 3, 0, tzinfo=UTC)
    command = RunCommand(
        command_id=CommandId.parse("command-1"),
        identity=identity,
        request=request,
        priority=QueuePriority.NORMAL,
        available_at=timestamp,
        created_at=timestamp,
    )

    restored = run_command_from_document(
        run_command_to_document(command)
    )

    assert restored.command_id == command.command_id
    assert restored.identity == identity
    assert restored.request.target == ExecutionTarget("research-graph", 4)
    assert restored.request.input == request.input
    assert restored.request.command == request.command
    assert restored.request.config == request.config
    assert restored.request.context == request.context
    assert restored.request.metadata == request.metadata
    assert restored.request.stream_modes == request.stream_modes
    assert restored.request.stream_subgraphs is True
    assert restored.request.priority is QueuePriority.NORMAL
    assert restored.request.timeout_seconds == 321
    assert restored.request.checkpoint_namespace == "supervisor/research"
    assert restored.request.checkpoint_id == "checkpoint-9"
    assert restored.available_at == timestamp
    assert restored.created_at == timestamp


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("stream_modes", ["messages", 4]),
        ("timeout_seconds", "soon"),
        ("priority", "interactive"),
        ("stream_subgraphs", "true"),
    ],
)
def test_run_command_codec_rejects_invalid_request_field(
    field: str,
    value: object,
) -> None:
    document = {
        "command_id": "command-1",
        "identity": {
            "workspace_id": "workspace",
            "project_id": "project",
            "application_id": "application",
            "revision_id": "revision",
            "deployment_id": "deployment",
            "assistant_id": "assistant",
            "thread_id": None,
            "run_id": "run-1",
            "attempt_id": "attempt-1",
        },
        "request": {
            "target": {
                "graph_id": "graph",
                "assistant_version": 1,
            },
            "stream_modes": ["values"],
            "stream_subgraphs": False,
            "priority": 100,
            "timeout_seconds": 1800,
        },
        "priority": 100,
    }
    request = document["request"]
    assert isinstance(request, dict)
    request[field] = value

    with pytest.raises(RuntimeFailure, match=field):
        run_command_from_document(document)


def test_run_command_codec_rejects_missing_identity_object() -> None:
    with pytest.raises(RuntimeFailure, match="identity"):
        run_command_from_document(
            {
                "command_id": "command-1",
                "identity": None,
                "request": {},
                "priority": 100,
            }
        )
