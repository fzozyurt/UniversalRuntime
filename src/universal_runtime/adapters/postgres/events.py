from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from universal_runtime.adapters.postgres.models import RuntimeEventRow
from universal_runtime.domain.events import RuntimeEvent, RuntimeEventDraft, TraceContext
from universal_runtime.domain.identity import (
    ApplicationId,
    ApplicationScope,
    AssistantId,
    AttemptId,
    DeploymentId,
    EventId,
    ExecutionIdentity,
    ProjectId,
    RevisionId,
    RunId,
    ThreadId,
    WorkspaceId,
)
from universal_runtime.ports.events import EventJournal, EventReplay


class PostgresEventJournal(EventJournal, EventReplay):
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def append(self, draft: RuntimeEventDraft) -> RuntimeEvent:
        identity = draft.identity
        async with self._sessions() as session:
            async with session.begin():
                await session.execute(
                    text("SELECT pg_advisory_xact_lock(hashtextextended(:run_id, 0))"),
                    {"run_id": str(identity.run_id)},
                )
                result = await session.execute(
                    select(func.max(RuntimeEventRow.sequence))
                    .where(RuntimeEventRow.run_id == str(identity.run_id))
                    .with_for_update()
                )
                next_sequence = int(result.scalar_one_or_none() or -1) + 1
                event = RuntimeEvent(
                    EventId.new(),
                    next_sequence,
                    datetime.now(UTC),
                    identity,
                    draft.type,
                    draft.namespace,
                    draft.data,
                    draft.native,
                    TraceContext(),
                )
                row = RuntimeEventRow(
                    id=str(event.event_id),
                    run_id=str(identity.run_id),
                    event_id=str(event.event_id),
                    sequence=event.sequence,
                    event_type=str(event.type),
                    timestamp=event.timestamp,
                    identity_json={
                        "workspace_id": str(identity.scope.workspace_id),
                        "project_id": str(identity.scope.project_id),
                        "application_id": str(identity.scope.application_id),
                        "revision_id": str(identity.scope.revision_id),
                        "deployment_id": str(identity.scope.deployment_id),
                        "assistant_id": str(identity.assistant_id),
                        "thread_id": str(identity.thread_id) if identity.thread_id else None,
                        "run_id": str(identity.run_id),
                        "attempt_id": str(identity.attempt_id),
                    },
                    namespace=list(event.namespace),
                    data=event.data,
                    trace={"trace_id": event.trace.trace_id, "span_id": event.trace.span_id},
                    native=event.native,
                )
                session.add(row)
            return event

    async def replay(self, run_id: RunId, *, after_sequence: int = -1) -> tuple[RuntimeEvent, ...]:
        async with self._sessions() as session:
            result = await session.execute(
                select(RuntimeEventRow)
                .where(
                    RuntimeEventRow.run_id == str(run_id), RuntimeEventRow.sequence > after_sequence
                )
                .order_by(RuntimeEventRow.sequence)
            )
            return tuple(_to_event(row) for row in result.scalars())


def _to_event(row: RuntimeEventRow) -> RuntimeEvent:
    raw = row.identity_json
    scope = ApplicationScope(
        WorkspaceId.parse(str(raw["workspace_id"])),
        ProjectId.parse(str(raw["project_id"])),
        ApplicationId.parse(str(raw["application_id"])),
        RevisionId.parse(str(raw["revision_id"])),
        DeploymentId.parse(str(raw["deployment_id"])),
    )
    identity = ExecutionIdentity(
        scope,
        AssistantId.parse(str(raw["assistant_id"])),
        RunId.parse(str(raw["run_id"])),
        AttemptId.parse(str(raw["attempt_id"])),
        ThreadId.parse(str(raw["thread_id"])) if raw.get("thread_id") else None,
    )
    return RuntimeEvent(
        EventId.parse(row.event_id),
        row.sequence,
        row.timestamp,
        identity,
        row.event_type,
        tuple(row.namespace),
        row.data,
        row.native,
        TraceContext(row.trace.get("trace_id"), row.trace.get("span_id")),
    )
