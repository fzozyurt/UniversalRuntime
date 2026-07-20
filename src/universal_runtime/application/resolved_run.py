from __future__ import annotations

from datetime import UTC, datetime
from typing import Protocol

from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.domain.events import RuntimeEventDraft, RuntimeEventType
from universal_runtime.domain.execution import ExecutionRequest, Run, RunCommand, RunError
from universal_runtime.domain.identity import CommandId
from universal_runtime.ports.events import EventJournal
from universal_runtime.ports.outbox import OutboxMessage, OutboxRepository
from universal_runtime.ports.queue import RunCommandQueue
from universal_runtime.ports.repositories import RunRepository, ThreadRepository


class ResolvedRunDependencies(Protocol):
    _threads: ThreadRepository
    _runs: RunRepository
    _commands: RunCommandQueue
    _journal: EventJournal
    _outbox: OutboxRepository | None


async def start_resolved_run(
    service: ResolvedRunDependencies,
    request: ExecutionRequest,
    *,
    outbox: OutboxRepository | None = None,
) -> Run:
    """Persist and queue an already resolved execution request exactly once."""
    now = datetime.now(UTC)
    thread_id = request.identity.thread_id
    if thread_id is not None:
        thread = await service._threads.get(str(thread_id))
        if thread.status.value == "busy":
            raise RuntimeFailure(
                ErrorCode.THREAD_BUSY,
                f"thread is busy: {thread_id}",
            )
        await service._threads.update(thread.mark_busy(now))
    run = Run(
        identity=request.identity,
        metadata=request.metadata,
        created_at=now,
        updated_at=now,
        target=request.target,
    )
    created = await service._runs.create(run)
    try:
        selected_outbox = outbox or service._outbox
        await service._journal.append(
            RuntimeEventDraft(
                request.identity,
                RuntimeEventType.RUN_QUEUED,
                data={
                    "assistant_id": str(request.identity.assistant_id),
                    "assistant_version": request.target.assistant_version,
                    "graph_id": request.target.graph_id,
                },
            )
        )
        if selected_outbox is not None:
            await selected_outbox.append(
                OutboxMessage(
                    message_id=CommandId.new(),
                    topic="rt.local.run.commands.v1",
                    key=(
                        f"{request.identity.application_id}:"
                        f"{request.identity.thread_id or 'stateless'}"
                    ),
                    payload={
                        "run_id": str(request.identity.run_id),
                        "assistant_id": str(request.identity.assistant_id),
                        "assistant_version": request.target.assistant_version,
                        "graph_id": request.target.graph_id,
                        "thread_id": (
                            str(request.identity.thread_id)
                            if request.identity.thread_id is not None
                            else None
                        ),
                        "input": request.input,
                        "command": request.command,
                        "config": request.config,
                        "context": request.context,
                        "metadata": request.metadata,
                        "stream_modes": list(request.stream_modes),
                    },
                    created_at=now,
                )
            )
        else:
            await service._commands.publish(
                RunCommand(
                    command_id=CommandId.new(),
                    identity=request.identity,
                    request=request,
                    priority=request.priority,
                    available_at=now,
                    created_at=now,
                )
            )
    except Exception:
        await service._runs.update(
            created.fail(
                RunError(
                    "RUN_QUEUE_FAILED",
                    "run command could not be queued",
                ),
                datetime.now(UTC),
            )
        )
        if thread_id is not None:
            thread = await service._threads.get(str(thread_id))
            await service._threads.update(
                thread.mark_error(datetime.now(UTC))
            )
        raise
    return created
