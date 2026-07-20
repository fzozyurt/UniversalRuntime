from __future__ import annotations

import logging
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

from universal_runtime.application.runtime_service import RuntimeExecutionService
from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.domain.events import RuntimeEventDraft, RuntimeEventType
from universal_runtime.domain.execution import (
    ExecutionRequest,
    Run,
    RunCommand,
    RunStatus,
)
from universal_runtime.domain.identity import CommandId
from universal_runtime.ports.cancellation import RunCancellation
from universal_runtime.ports.outbox import OutboxRepository
from universal_runtime.ports.repositories import ThreadApplicationBinder
from universal_runtime.ports.submission import RunSubmissionStore

_LOGGER = logging.getLogger(__name__)
_TERMINAL_STATUSES = {
    RunStatus.SUCCESS,
    RunStatus.ERROR,
    RunStatus.TIMEOUT,
    RunStatus.CANCELLED,
}


class ManagedExecutionService(RuntimeExecutionService):
    """Platform execution policy layered over the framework-neutral core service."""

    def __init__(
        self,
        *,
        thread_binder: ThreadApplicationBinder | None = None,
        cancellation: RunCancellation | None = None,
        submission: RunSubmissionStore | None = None,
        **dependencies: Any,
    ) -> None:
        super().__init__(**dependencies)
        self._thread_binder = thread_binder
        self._remote_cancellation = cancellation
        self._submission = submission

    async def start_run(
        self,
        request: ExecutionRequest,
        *,
        outbox: OutboxRepository | None = None,
    ) -> Run:
        resolved = await self._resolve_request(request)
        if self._submission is None or outbox is not None:
            thread_id = resolved.identity.thread_id
            if thread_id is not None and self._thread_binder is not None:
                await self._thread_binder.bind(
                    str(thread_id),
                    resolved.identity.scope,
                )
            return await super().start_run(resolved, outbox=outbox)

        now = datetime.now(UTC)
        busy_thread = None
        thread_id = resolved.identity.thread_id
        if thread_id is not None:
            thread = await self._threads.get(str(thread_id))
            if thread.status.value == "busy":
                raise RuntimeFailure(
                    ErrorCode.THREAD_BUSY,
                    f"thread is busy: {thread_id}",
                )
            busy_thread = thread.mark_busy(now)

        run = Run(
            identity=resolved.identity,
            metadata=resolved.metadata,
            created_at=now,
            updated_at=now,
            target=resolved.target,
        )
        command = RunCommand(
            command_id=CommandId.new(),
            identity=resolved.identity,
            request=resolved,
            priority=resolved.priority,
            available_at=now,
            created_at=now,
        )
        created = await self._submission.submit(
            run,
            command,
            thread=busy_thread,
        )
        await self._journal.append(
            RuntimeEventDraft(
                resolved.identity,
                RuntimeEventType.RUN_QUEUED,
                data={
                    "assistant_id": str(resolved.identity.assistant_id),
                    "assistant_version": resolved.target.assistant_version,
                    "graph_id": resolved.target.graph_id,
                    "outbox": True,
                },
            )
        )
        return created

    async def resume_run(self, request: ExecutionRequest) -> Run:
        run = await self._runs.get(str(request.identity.run_id))
        if run.status is not RunStatus.INTERRUPTED:
            raise RuntimeFailure(
                ErrorCode.INVALID_EXECUTION_INPUT,
                "run is not interrupted",
            )
        resolved = await self._resolve_request(
            replace(request, target=run.target),
            pinned_scope=run.identity.scope,
            pinned_target=run.target,
        )
        now = datetime.now(UTC)
        try:
            resumed = run.resume(resolved.identity, now)
        except ValueError as exc:
            raise RuntimeFailure(
                ErrorCode.INVALID_EXECUTION_INPUT,
                str(exc),
            ) from exc

        busy_thread = None
        if resumed.thread_id is not None:
            thread = await self._threads.get(str(resumed.thread_id))
            if thread.status.value == "busy":
                raise RuntimeFailure(
                    ErrorCode.THREAD_BUSY,
                    f"thread is busy: {resumed.thread_id}",
                )
            busy_thread = thread.mark_busy(now)

        command_request = replace(resolved, identity=resumed.identity)
        command = RunCommand(
            command_id=CommandId.new(),
            identity=resumed.identity,
            request=command_request,
            priority=command_request.priority,
            available_at=now,
            created_at=now,
        )
        if self._submission is None:
            await self._runs.update(resumed)
            await self._commands.publish(command)
        else:
            await self._submission.resume(
                resumed,
                command,
                thread=busy_thread,
            )
        await self._journal.append(
            RuntimeEventDraft(
                resumed.identity,
                RuntimeEventType.RUN_QUEUED,
                data={
                    "resume": True,
                    "attempt_id": str(resumed.identity.attempt_id),
                    "graph_id": resumed.target.graph_id,
                    "assistant_version": resumed.target.assistant_version,
                    "outbox": self._submission is not None,
                },
            )
        )
        return resumed

    async def cancel_run(self, run_id: str) -> Run:
        run = await self._runs.get(run_id)
        if run.status in _TERMINAL_STATUSES:
            return run

        now = datetime.now(UTC)
        cancelled = run.cancel(now)
        await self._runs.update(cancelled)

        owner_notified = False
        if self._remote_cancellation is not None:
            try:
                owner_notified = await self._remote_cancellation.cancel(cancelled)
            except Exception:
                # Cancellation intent is already durable. A transient Worker/Gateway
                # failure must not turn a user cancellation into an HTTP failure.
                _LOGGER.exception(
                    "remote run cancellation failed run_id=%s",
                    run_id,
                )

        adapter = self._active_adapters.get(str(run.run_id))
        if adapter is not None:
            try:
                await adapter.cancel(
                    ExecutionRequest(identity=run.identity, target=run.target)
                )
                owner_notified = True
            except Exception:
                _LOGGER.exception(
                    "local adapter cancellation failed run_id=%s",
                    run_id,
                )

        if run.thread_id is not None:
            thread = await self._threads.get(str(run.thread_id))
            await self._threads.update(thread.mark_idle(datetime.now(UTC)))
        await self._journal.append(
            RuntimeEventDraft(
                run.identity,
                RuntimeEventType.RUN_CANCELLED,
                data={
                    "run_id": str(run.run_id),
                    "status": str(cancelled.status),
                    "execution_owner_notified": owner_notified,
                },
            )
        )
        return cancelled
