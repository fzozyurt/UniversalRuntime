from __future__ import annotations

import os

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


def deployment_identity(
    assistant_id: AssistantId,
    run_id: RunId,
    thread_id: ThreadId | None,
) -> ExecutionIdentity:
    """Create a run identity from the Gateway's active deployment contract."""

    workspace = os.environ.get("UR_WORKSPACE_ID") or os.environ.get(
        "UR_WORKSPACE_KEY",
        "default",
    )
    application = os.environ.get("UR_APPLICATION_ID", "default")
    revision = (
        os.environ.get("UR_REVISION_ID")
        or os.environ.get("ARTIFACT_VERSION")
        or "development"
    )
    deployment = os.environ.get("UR_DEPLOYMENT_ID", f"{application}-local")
    return ExecutionIdentity(
        ApplicationScope(
            WorkspaceId.parse(workspace),
            ProjectId.parse(os.environ.get("UR_PROJECT_ID", "default")),
            ApplicationId.parse(application),
            RevisionId.parse(revision),
            DeploymentId.parse(deployment),
        ),
        assistant_id,
        run_id,
        AttemptId.new(),
        thread_id,
    )
