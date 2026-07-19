from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import NewType

WorkspaceId = NewType("WorkspaceId", str)
ProjectId = NewType("ProjectId", str)
ApplicationId = NewType("ApplicationId", str)
RevisionId = NewType("RevisionId", str)
DeploymentId = NewType("DeploymentId", str)
AssistantId = NewType("AssistantId", str)
ThreadId = NewType("ThreadId", str)
RunId = NewType("RunId", str)
AttemptId = NewType("AttemptId", str)


def new_identifier() -> str:
    """Return a UUIDv7 string when supported, otherwise a UUID-compatible ID."""
    uuid7 = getattr(uuid, "uuid7", None)
    return str(uuid7() if uuid7 is not None else uuid.uuid4())


@dataclass(frozen=True, slots=True)
class ExecutionIdentity:
    workspace_id: WorkspaceId
    project_id: ProjectId
    application_id: ApplicationId
    revision_id: RevisionId
    deployment_id: DeploymentId
    assistant_id: AssistantId
    run_id: RunId
    attempt_id: AttemptId
    thread_id: ThreadId | None = None
