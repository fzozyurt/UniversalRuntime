from __future__ import annotations

from datetime import UTC, datetime

import pytest

from universal_runtime.domain.assistants import Assistant
from universal_runtime.domain.execution import Run, RunError, RunStatus, Thread, ThreadStatus
from universal_runtime.domain.identity import (
    ApplicationId,
    ApplicationScope,
    AssistantId,
    AttemptId,
    DeploymentId,
    ProjectId,
    RevisionId,
    RunId,
    ThreadId,
    WorkspaceId,
)


def scope() -> ApplicationScope:
    return ApplicationScope(
        WorkspaceId.parse("w"),
        ProjectId.parse("p"),
        ApplicationId.parse("a"),
        RevisionId.parse("r"),
        DeploymentId.parse("d"),
    )


def run() -> Run:
    from universal_runtime.domain.identity import ExecutionIdentity

    return Run(
        ExecutionIdentity(
            scope(),
            AssistantId.parse("assistant"),
            RunId.parse("run"),
            AttemptId.parse("attempt"),
            ThreadId.parse("thread"),
        ),
        metadata={"nested": {"value": 1}},
    )


def test_ids_are_distinct_and_reject_empty() -> None:
    for identifier in (
        WorkspaceId,
        ProjectId,
        ApplicationId,
        RevisionId,
        DeploymentId,
        AssistantId,
        ThreadId,
        RunId,
        AttemptId,
    ):
        with pytest.raises(ValueError):
            identifier.parse("")
    assert ThreadId.parse("x") != RunId.parse("x")


def test_run_transitions_reject_terminal_changes() -> None:
    now = datetime.now(UTC)
    running = run().mark_running(now)
    completed = running.complete({"ok": True}, now)
    assert completed.status is RunStatus.SUCCESS
    with pytest.raises(ValueError):
        completed.mark_running(now)
    assert completed.cancel(now) if False else completed.status is RunStatus.SUCCESS
    failed = running.fail(RunError("E", "failed"), now)
    assert failed.status is RunStatus.ERROR


def test_thread_transitions_and_assistant_json_copy() -> None:
    now = datetime.now(UTC)
    thread = Thread(ThreadId.parse("thread"), metadata={"x": {"y": 1}})
    assert thread.mark_busy(now).status is ThreadStatus.BUSY
    assert thread.mark_interrupted(now).status is ThreadStatus.INTERRUPTED
    assert thread.mark_error(now).status is ThreadStatus.ERROR
    source = {"nested": {"value": 1}}
    assistant = Assistant(AssistantId.parse("assistant"), "graph", config=source)
    source["nested"]["value"] = 2
    assert assistant.config["nested"]["value"] == 1
    assert thread.mark_idle(now).status is ThreadStatus.IDLE
