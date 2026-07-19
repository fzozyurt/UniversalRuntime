from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

from universal_runtime.domain.identity import AssistantId, RunId, ThreadId, new_identifier

ThreadStatus = Literal["idle", "busy", "interrupted", "error"]
RunStatus = Literal["pending", "running", "interrupted", "success", "error", "timeout", "cancelled"]


@dataclass(frozen=True, slots=True)
class AssistantRecord:
    assistant_id: AssistantId
    graph_id: str
    version: int = 1
    name: str | None = None
    config: dict[str, Any] = field(default_factory=dict)
    context: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ThreadRecord:
    thread_id: ThreadId
    status: ThreadStatus = "idle"
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class RunRecord:
    run_id: RunId
    thread_id: ThreadId | None
    assistant_id: AssistantId
    status: RunStatus = "pending"
    metadata: dict[str, Any] = field(default_factory=dict)


def generated_thread_id() -> ThreadId:
    return ThreadId(new_identifier())


def generated_run_id() -> RunId:
    return RunId(new_identifier())
