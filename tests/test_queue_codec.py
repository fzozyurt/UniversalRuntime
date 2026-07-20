from datetime import UTC, datetime

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

    restored = run_command_from_document(run_command_to_document(command))

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
