from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from universal_runtime.adapters.postgres.models import (
    ApplicationRow,
    AssistantRow,
    AssistantVersionRow,
    GraphRow,
)
from universal_runtime.domain.assistants import Assistant
from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.domain.identity import ApplicationId, ProjectId, WorkspaceId


class PostgresGatewayAssistantRepository:
    """Tenant-scoped assistant repository for the shared compatibility Gateway."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        *,
        workspace_id: WorkspaceId,
        project_id: ProjectId,
        default_application_id: ApplicationId | None = None,
    ) -> None:
        self._sessions = sessions
        self._workspace_id = workspace_id
        self._project_id = project_id
        self._default_application_id = default_application_id

    async def _application_id_for(self, assistant: Assistant) -> str:
        metadata_application = assistant.metadata.get("runtime.application_id")
        application_id = (
            str(metadata_application)
            if metadata_application is not None
            else (
                str(self._default_application_id)
                if self._default_application_id is not None
                else ""
            )
        )
        if not application_id:
            raise RuntimeFailure(
                ErrorCode.INVALID_EXECUTION_INPUT,
                "shared Gateway assistant creation requires runtime.application_id",
            )
        async with self._sessions() as session:
            application = await session.get(ApplicationRow, application_id)
            if (
                application is None
                or application.workspace_id != str(self._workspace_id)
                or application.project_id != str(self._project_id)
            ):
                raise RuntimeFailure(
                    ErrorCode.RESOURCE_NOT_FOUND,
                    f"application not found: {application_id}",
                )
            graph = (
                await session.execute(
                    select(GraphRow).where(
                        GraphRow.application_id == application_id,
                        GraphRow.graph_id == assistant.graph_id,
                    )
                )
            ).scalar_one_or_none()
            if graph is None:
                raise RuntimeFailure(
                    ErrorCode.RESOURCE_NOT_FOUND,
                    f"graph not found: {application_id}/{assistant.graph_id}",
                )
        return application_id

    async def _row(self, assistant_id: str) -> tuple[AssistantRow, AssistantVersionRow]:
        async with self._sessions() as session:
            result = await session.execute(
                select(AssistantRow, AssistantVersionRow)
                .join(ApplicationRow, ApplicationRow.id == AssistantRow.application_id)
                .join(
                    AssistantVersionRow,
                    AssistantVersionRow.assistant_id == AssistantRow.id,
                )
                .where(
                    AssistantRow.id == str(assistant_id),
                    AssistantVersionRow.version == AssistantRow.active_version,
                    ApplicationRow.workspace_id == str(self._workspace_id),
                    ApplicationRow.project_id == str(self._project_id),
                )
            )
            pair = result.first()
            if pair is None:
                raise RuntimeFailure(
                    ErrorCode.RESOURCE_NOT_FOUND,
                    f"assistant not found: {assistant_id}",
                )
            return pair[0], pair[1]

    @staticmethod
    def _domain(row: AssistantRow, version: AssistantVersionRow) -> Assistant:
        metadata = dict(version.metadata_json)
        metadata.setdefault("runtime.application_id", row.application_id)
        return Assistant(
            assistant_id=__import__(
                "universal_runtime.domain.identity",
                fromlist=["AssistantId"],
            ).AssistantId.parse(row.id),
            graph_id=row.graph_id,
            version=version.version,
            name=metadata.pop("name", None),
            config=version.config,
            context=version.context,
            metadata=metadata,
        )

    async def create(self, assistant: Assistant) -> Assistant:
        application_id = await self._application_id_for(assistant)
        async with self._sessions() as session:
            async with session.begin():
                existing = await session.get(AssistantRow, str(assistant.assistant_id))
                if existing is not None:
                    if existing.application_id != application_id:
                        raise RuntimeFailure(
                            ErrorCode.INVALID_EXECUTION_INPUT,
                            "assistant identifier is already used by another application",
                        )
                    return await self.get(str(assistant.assistant_id))
                session.add(
                    AssistantRow(
                        id=str(assistant.assistant_id),
                        application_id=application_id,
                        graph_id=assistant.graph_id,
                        active_version=assistant.version,
                    )
                )
                session.add(
                    AssistantVersionRow(
                        id=f"{assistant.assistant_id}:{assistant.version}",
                        assistant_id=str(assistant.assistant_id),
                        version=assistant.version,
                        config=assistant.config,
                        context=assistant.context,
                        metadata_json={**assistant.metadata, "name": assistant.name},
                    )
                )
        return await self.get(str(assistant.assistant_id))

    async def get(self, assistant_id: str) -> Assistant:
        row, version = await self._row(assistant_id)
        return self._domain(row, version)

    async def all(self) -> tuple[Assistant, ...]:
        async with self._sessions() as session:
            ids = (
                await session.execute(
                    select(AssistantRow.id)
                    .join(ApplicationRow, ApplicationRow.id == AssistantRow.application_id)
                    .where(
                        ApplicationRow.workspace_id == str(self._workspace_id),
                        ApplicationRow.project_id == str(self._project_id),
                    )
                    .order_by(AssistantRow.created_at.desc())
                )
            ).scalars().all()
        return tuple([await self.get(str(assistant_id)) for assistant_id in ids])

    async def versions(self, assistant_id: str) -> tuple[Assistant, ...]:
        current, _ = await self._row(assistant_id)
        async with self._sessions() as session:
            versions = (
                await session.execute(
                    select(AssistantVersionRow)
                    .where(AssistantVersionRow.assistant_id == str(assistant_id))
                    .order_by(AssistantVersionRow.version.desc())
                )
            ).scalars().all()
        return tuple(self._domain(current, version) for version in versions)

    async def update(self, assistant_id: str, assistant: Assistant) -> Assistant:
        row, _ = await self._row(assistant_id)
        if assistant.graph_id != row.graph_id:
            application_id = await self._application_id_for(assistant)
            if application_id != row.application_id:
                raise RuntimeFailure(
                    ErrorCode.INVALID_EXECUTION_INPUT,
                    "assistant cannot move between applications",
                )
        async with self._sessions() as session:
            async with session.begin():
                locked = (
                    await session.execute(
                        select(AssistantRow)
                        .where(AssistantRow.id == str(assistant_id))
                        .with_for_update()
                    )
                ).scalar_one()
                version = locked.active_version + 1
                session.add(
                    AssistantVersionRow(
                        id=f"{assistant_id}:{version}",
                        assistant_id=str(assistant_id),
                        version=version,
                        config=assistant.config,
                        context=assistant.context,
                        metadata_json={**assistant.metadata, "name": assistant.name},
                    )
                )
                locked.graph_id = assistant.graph_id
                locked.active_version = version
        return await self.get(str(assistant_id))

    async def set_latest(self, assistant_id: str, version: int) -> Assistant:
        await self._row(assistant_id)
        async with self._sessions() as session:
            async with session.begin():
                row = await session.get(AssistantRow, str(assistant_id))
                version_row = await session.get(
                    AssistantVersionRow, f"{assistant_id}:{version}"
                )
                if row is None or version_row is None:
                    raise RuntimeFailure(
                        ErrorCode.RESOURCE_NOT_FOUND,
                        f"assistant version not found: {assistant_id}/{version}",
                    )
                row.active_version = version
        return await self.get(str(assistant_id))

    async def delete(self, assistant_id: str, *, delete_threads: bool = False) -> None:
        del delete_threads
        await self._row(assistant_id)
        async with self._sessions() as session:
            async with session.begin():
                versions = (
                    await session.execute(
                        select(AssistantVersionRow).where(
                            AssistantVersionRow.assistant_id == str(assistant_id)
                        )
                    )
                ).scalars().all()
                for version in versions:
                    await session.delete(version)
                row = await session.get(AssistantRow, str(assistant_id))
                if row is not None:
                    await session.delete(row)

    async def count(self, *, graph_id: str | None = None) -> int:
        items = await self.all()
        return sum(1 for item in items if graph_id is None or item.graph_id == graph_id)
