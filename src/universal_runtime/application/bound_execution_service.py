from __future__ import annotations

from typing import Any

from universal_runtime.application.runtime_service import RuntimeExecutionService
from universal_runtime.domain.execution import ExecutionRequest, Run
from universal_runtime.ports.outbox import OutboxRepository
from universal_runtime.ports.repositories import ThreadApplicationBinder


class ApplicationBoundExecutionService(RuntimeExecutionService):
    """Execution policy that binds a compatibility thread on its first run."""

    def __init__(
        self,
        *,
        thread_binder: ThreadApplicationBinder,
        **dependencies: Any,
    ) -> None:
        super().__init__(**dependencies)
        self._application_thread_binder = thread_binder

    async def start_run(
        self,
        request: ExecutionRequest,
        *,
        outbox: OutboxRepository | None = None,
    ) -> Run:
        resolved = await self._resolve_request(request)
        thread_id = resolved.identity.thread_id
        if thread_id is not None:
            await self._application_thread_binder.bind(
                str(thread_id), resolved.identity.scope
            )
        return await super().start_run(resolved, outbox=outbox)
