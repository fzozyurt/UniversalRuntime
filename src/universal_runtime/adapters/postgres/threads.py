from __future__ import annotations

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from universal_runtime.adapters.postgres.models import ThreadRow
from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.domain.execution import Thread, ThreadStatus
from universal_runtime.domain.identity import ApplicationScope, ProjectId, ThreadId, WorkspaceId


class SharedPostgresThreadRepository:
    """Workspace/project thread repository with first-run application binding."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        *,
        workspace_id: WorkspaceId,
        project_id: ProjectId,
    ) -> None:
        self._sessions = sessions
        self._workspace_id = workspace_id
        self._project_id = project_id

    def _matches_tenant(self, row: ThreadRow) -> bool:
        return row.workspace_id == str(self._workspace_id) and row.project_id == str(
            self._project_id
        )

    async def create(self, thread: Thread) -> Thread:
        async with self._sessions() as session:
            async with session.begin():
                await session.execute(
                    text(
                        "INSERT INTO rt_exec.threads "
                        "(id, workspace_id, project_id, application_id, status, metadata) "
                        "VALUES (:id, :workspace_id, :project_id, NULL, :status, "
                        "CAST(:metadata AS jsonb))"
                    ),
                    {
                        "id": str(thread.thread_id),
                        "workspace_id": str(self._workspace_id),
                        "project_id": str(self._project_id),
                        "status": thread.status.value,
                        "metadata": __import__("json").dumps(thread.metadata),
                    },
                )
        return thread

    async def get(self, thread_id: str) -> Thread:
        async with self._sessions() as session:
            row = await session.get(ThreadRow, str(thread_id))
            if row is None or not self._matches_tenant(row):
                raise RuntimeFailure(ErrorCode.RESOURCE_NOT_FOUND, f"thread not found: {thread_id}")
            return Thread(
                ThreadId.parse(row.id),
                ThreadStatus(row.status),
                row.metadata_json,
                row.created_at,
                row.updated_at,
            )

    async def update(self, thread: Thread) -> Thread:
        async with self._sessions() as session:
            async with session.begin():
                row = await session.get(ThreadRow, str(thread.thread_id))
                if row is None or not self._matches_tenant(row):
                    raise RuntimeFailure(
                        ErrorCode.RESOURCE_NOT_FOUND,
                        f"thread not found: {thread.thread_id}",
                    )
                row.status = thread.status.value
                row.metadata_json = thread.metadata
        return thread

    async def delete(self, thread_id: str) -> None:
        async with self._sessions() as session:
            async with session.begin():
                row = await session.get(ThreadRow, str(thread_id))
                if row is None or not self._matches_tenant(row):
                    raise RuntimeFailure(
                        ErrorCode.RESOURCE_NOT_FOUND, f"thread not found: {thread_id}"
                    )
                await session.delete(row)

    async def search(
        self,
        *,
        metadata: dict[str, object] | None = None,
        status: str | None = None,
        limit: int = 10,
        offset: int = 0,
    ) -> tuple[Thread, ...]:
        async with self._sessions() as session:
            query = (
                select(ThreadRow)
                .where(
                    ThreadRow.workspace_id == str(self._workspace_id),
                    ThreadRow.project_id == str(self._project_id),
                )
                .order_by(ThreadRow.created_at.desc())
                .limit(limit)
                .offset(offset)
            )
            if status is not None:
                query = query.where(ThreadRow.status == status)
            rows = (await session.execute(query)).scalars().all()
            return tuple(
                Thread(
                    ThreadId.parse(row.id),
                    ThreadStatus(row.status),
                    row.metadata_json,
                    row.created_at,
                    row.updated_at,
                )
                for row in rows
                if all(
                    row.metadata_json.get(key) == value for key, value in (metadata or {}).items()
                )
            )

    async def count(
        self,
        *,
        metadata: dict[str, object] | None = None,
        status: str | None = None,
    ) -> int:
        return len(
            await self.search(
                metadata=metadata,
                status=status,
                limit=100_000,
                offset=0,
            )
        )


class PostgresThreadApplicationBinder:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def bind(self, thread_id: str, scope: ApplicationScope) -> None:
        async with self._sessions() as session:
            async with session.begin():
                row = (
                    await session.execute(
                        select(ThreadRow).where(ThreadRow.id == str(thread_id)).with_for_update()
                    )
                ).scalar_one_or_none()
                if row is None:
                    raise RuntimeFailure(
                        ErrorCode.RESOURCE_NOT_FOUND, f"thread not found: {thread_id}"
                    )
                if row.workspace_id != str(scope.workspace_id) or row.project_id != str(
                    scope.project_id
                ):
                    raise RuntimeFailure(
                        ErrorCode.RESOURCE_NOT_FOUND, f"thread not found: {thread_id}"
                    )
                if row.application_id is None:
                    row.application_id = str(scope.application_id)
                    return
                if row.application_id != str(scope.application_id):
                    raise RuntimeFailure(
                        ErrorCode.INVALID_EXECUTION_INPUT,
                        "thread is already bound to another application",
                        details={
                            "thread_id": str(thread_id),
                            "bound_application_id": row.application_id,
                            "requested_application_id": str(scope.application_id),
                        },
                    )
