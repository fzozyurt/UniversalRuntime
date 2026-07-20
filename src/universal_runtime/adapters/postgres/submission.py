from __future__ import annotations

from collections.abc import Mapping

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from universal_runtime.adapters.postgres.models import (
    OutboxEventRow,
    RunRow,
    ThreadRow,
)
from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.domain.execution import QueuePriority, Run, RunCommand, Thread
from universal_runtime.ports.submission import RunSubmissionStore
from universal_runtime.transport.queue_codec import run_command_to_document


class PostgresRunSubmissionStore(RunSubmissionStore):
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        *,
        topics: Mapping[QueuePriority, str],
    ) -> None:
        self._sessions = sessions
        self._topics = dict(topics)

    @staticmethod
    def _run_row(run: Run) -> RunRow:
        identity = run.identity
        return RunRow(
            id=str(identity.run_id),
            workspace_id=str(identity.workspace_id),
            project_id=str(identity.project_id),
            application_id=str(identity.application_id),
            revision_id=str(identity.revision_id),
            deployment_id=str(identity.deployment_id),
            assistant_id=str(identity.assistant_id),
            assistant_version=run.target.assistant_version,
            graph_id=run.target.graph_id,
            thread_id=str(identity.thread_id) if identity.thread_id else None,
            attempt_id=str(identity.attempt_id),
            status=run.status.value,
            metadata_json=run.metadata,
            result=run.result,
            error=(
                {
                    "code": run.error.code,
                    "message": run.error.message,
                    "retryable": run.error.retryable,
                    "details": run.error.details,
                }
                if run.error
                else None
            ),
            created_at=run.created_at,
            updated_at=run.updated_at,
        )

    async def submit(
        self,
        run: Run,
        command: RunCommand,
        *,
        thread: Thread | None,
    ) -> Run:
        try:
            async with self._sessions() as session:
                async with session.begin():
                    existing = await session.get(RunRow, str(run.run_id))
                    if existing is not None:
                        return run

                    if thread is not None:
                        thread_row = (
                            await session.execute(
                                select(ThreadRow)
                                .where(ThreadRow.id == str(thread.thread_id))
                                .with_for_update()
                            )
                        ).scalar_one_or_none()
                        if thread_row is None:
                            raise RuntimeFailure(
                                ErrorCode.RESOURCE_NOT_FOUND,
                                f"thread not found: {thread.thread_id}",
                            )
                        identity = run.identity
                        if (
                            thread_row.workspace_id != str(identity.workspace_id)
                            or thread_row.project_id != str(identity.project_id)
                        ):
                            raise RuntimeFailure(
                                ErrorCode.RESOURCE_NOT_FOUND,
                                f"thread not found: {thread.thread_id}",
                            )
                        if thread_row.application_id is None:
                            thread_row.application_id = str(identity.application_id)
                        elif thread_row.application_id != str(identity.application_id):
                            raise RuntimeFailure(
                                ErrorCode.INVALID_EXECUTION_INPUT,
                                "thread is already bound to another application",
                                details={
                                    "thread_id": str(thread.thread_id),
                                    "bound_application_id": thread_row.application_id,
                                    "requested_application_id": str(identity.application_id),
                                },
                            )
                        if thread_row.status == "busy":
                            raise RuntimeFailure(
                                ErrorCode.THREAD_BUSY,
                                f"thread is busy: {thread.thread_id}",
                            )
                        thread_row.status = thread.status.value
                        thread_row.metadata_json = thread.metadata

                    session.add(self._run_row(run))
                    session.add(
                        OutboxEventRow(
                            id=str(command.command_id),
                            event_id=str(command.command_id),
                            aggregate_type="run_command",
                            aggregate_id=(
                                f"{run.identity.application_id}:"
                                f"{run.identity.thread_id or 'stateless'}"
                            ),
                            topic=self._topics[command.priority],
                            idempotency_key=f"run-command:{run.run_id}",
                            payload=run_command_to_document(command),
                            published_at=None,
                        )
                    )
                    await session.flush()
            return run
        except IntegrityError as exc:
            async with self._sessions() as session:
                existing = await session.get(RunRow, str(run.run_id))
                if existing is not None:
                    return run
            raise RuntimeFailure(
                ErrorCode.THREAD_BUSY,
                "thread already has an active run",
            ) from exc
