from __future__ import annotations

from dataclasses import dataclass, field

from universal_runtime.domain.assistants import Assistant
from universal_runtime.domain.execution import ExecutionTarget
from universal_runtime.domain.identity import (
    ApplicationId,
    ApplicationScope,
    DeploymentId,
    ProjectId,
    RevisionId,
    WorkspaceId,
)
from universal_runtime.domain.primitives.json_types import JsonObject


@dataclass(frozen=True, slots=True)
class GraphRegistration:
    graph_id: str
    entrypoint: str
    descriptor: JsonObject = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.graph_id.strip():
            raise ValueError("graph_id must not be empty")
        if not self.entrypoint.strip():
            raise ValueError("graph entrypoint must not be empty")


@dataclass(frozen=True, slots=True)
class ApplicationDeploymentRegistration:
    workspace_id: WorkspaceId
    project_id: ProjectId
    application_id: ApplicationId
    application_name: str
    revision_id: RevisionId
    deployment_id: DeploymentId
    environment: str
    image_digest: str
    graphs: tuple[GraphRegistration, ...]
    revision_metadata: JsonObject = field(default_factory=dict)
    activate_revision: bool = False

    def __post_init__(self) -> None:
        if not self.application_name.strip():
            raise ValueError("application_name must not be empty")
        if not self.environment.strip():
            raise ValueError("environment must not be empty")
        if not self.image_digest.strip():
            raise ValueError("image_digest must not be empty")
        if not self.graphs:
            raise ValueError("application deployment must expose at least one graph")
        graph_ids = [graph.graph_id for graph in self.graphs]
        if len(graph_ids) != len(set(graph_ids)):
            raise ValueError("application deployment contains duplicate graph_id values")

    @property
    def scope(self) -> ApplicationScope:
        return ApplicationScope(
            self.workspace_id,
            self.project_id,
            self.application_id,
            self.revision_id,
            self.deployment_id,
        )


@dataclass(frozen=True, slots=True)
class ResolvedExecutionPlan:
    scope: ApplicationScope
    assistant: Assistant
    target: ExecutionTarget

    def __post_init__(self) -> None:
        if self.assistant.graph_id != self.target.graph_id:
            raise ValueError("resolved assistant and execution target graph must match")
        if self.assistant.version != self.target.assistant_version:
            raise ValueError("resolved assistant version and execution target version must match")
