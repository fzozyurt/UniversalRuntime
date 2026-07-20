from __future__ import annotations

from typing import Protocol

from universal_runtime.domain.applications import (
    ApplicationDeploymentRegistration,
    ResolvedExecutionPlan,
)
from universal_runtime.domain.assistants import Assistant
from universal_runtime.domain.identity import AssistantId


class ApplicationDeploymentCatalog(Protocol):
    async def register(
        self,
        registration: ApplicationDeploymentRegistration,
    ) -> tuple[Assistant, ...]: ...


class ExecutionPlanResolver(Protocol):
    async def resolve(self, assistant_id: AssistantId) -> ResolvedExecutionPlan: ...
