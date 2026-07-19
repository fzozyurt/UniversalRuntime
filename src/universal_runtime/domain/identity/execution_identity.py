from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from universal_runtime.domain.identity.identifiers import (
    ApplicationId,
    AssistantId,
    AttemptId,
    DeploymentId,
    ProjectId,
    RevisionId,
    RunId,
    ThreadId,
    WorkspaceId,
)


@dataclass(frozen=True, slots=True)
class ApplicationScope:
    workspace_id: WorkspaceId
    project_id: ProjectId
    application_id: ApplicationId
    revision_id: RevisionId
    deployment_id: DeploymentId


@dataclass(frozen=True, slots=True, init=False)
class ExecutionIdentity:
    scope: ApplicationScope
    assistant_id: AssistantId
    run_id: RunId
    attempt_id: AttemptId
    thread_id: ThreadId | None

    def __init__(
        self,
        scope: ApplicationScope | WorkspaceId | None = None,
        assistant_id: AssistantId | None = None,
        run_id: RunId | None = None,
        attempt_id: AttemptId | None = None,
        thread_id: ThreadId | None = None,
        *,
        workspace_id: WorkspaceId | None = None,
        project_id: ProjectId | None = None,
        application_id: ApplicationId | None = None,
        revision_id: RevisionId | None = None,
        deployment_id: DeploymentId | None = None,
    ) -> None:
        if isinstance(scope, ApplicationScope):
            resolved_scope = scope
        else:
            resolved_workspace = workspace_id if scope is None else scope
            values = (resolved_workspace, project_id, application_id, revision_id, deployment_id)
            if any(value is None or not str(value) for value in values):
                raise ValueError("complete application scope is required")
            resolved_scope = ApplicationScope(
                cast(WorkspaceId, resolved_workspace),
                cast(ProjectId, project_id),
                cast(ApplicationId, application_id),
                cast(RevisionId, revision_id),
                cast(DeploymentId, deployment_id),
            )
        if assistant_id is None or run_id is None or attempt_id is None:
            raise ValueError("execution identity requires assistant, run and attempt IDs")
        object.__setattr__(self, "scope", resolved_scope)
        object.__setattr__(self, "assistant_id", assistant_id)
        object.__setattr__(self, "run_id", run_id)
        object.__setattr__(self, "attempt_id", attempt_id)
        object.__setattr__(self, "thread_id", thread_id)

    @property
    def workspace_id(self) -> WorkspaceId:
        return self.scope.workspace_id

    @property
    def project_id(self) -> ProjectId:
        return self.scope.project_id

    @property
    def application_id(self) -> ApplicationId:
        return self.scope.application_id

    @property
    def revision_id(self) -> RevisionId:
        return self.scope.revision_id

    @property
    def deployment_id(self) -> DeploymentId:
        return self.scope.deployment_id
