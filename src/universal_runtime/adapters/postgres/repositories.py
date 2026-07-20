from __future__ import annotations

import hashlib
import json
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from universal_runtime.adapters.postgres.models import (
    AssistantRow,
    AssistantVersionRow,
    RunRow,
    ThreadRow,
)
from universal_runtime.domain.assistants import Assistant
from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.domain.execution import (
    ExecutionTarget,
    Run,
    RunError,
    RunStatus,
    Thread,
    ThreadStatus,
)
from universal_runtime.domain.identity import (
    ApplicationId,
    ApplicationScope,
    AssistantId,
    AttemptId,
    ConfigRevisionId,
    DeploymentId,
    ExecutionIdentity,
    ProjectId,
    RevisionId,
    RunId,
    ThreadId,
    WorkspaceId,
)
from universal_runtime.ports.configuration import ConfigRevision


class PostgresAssistantRepository:
    def __init__(self, sessions: async_sessionmaker[AsyncSession], application_id: str) -> None:
        self._sessions, self._application_id = sessions, application_id

    async def create(self, assistant: Assistant) -> Assistant:
        """Create an assistant idempotently across concurrently starting Gateways."""
        try:
            async with self._sessions() as session:
                async with session.begin():
                    row = await session.get(AssistantRow, str(assistant.assistant_id))
                    if row is not None and row.application_id != self._application_id:
                        raise RuntimeFailure(
                            ErrorCode.INVALID_EXECUTION_INPUT,
                            "assistant identifier is already used by another application",
                        )
                    if row is None:
                        session.add(
                            AssistantRow(
                                id=str(assistant.assistant_id),
                                application_id=self._application_id,
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
        except IntegrityError:
            # Two Gateway replicas may register the same graph simultaneously.
            # The first committed row is the authoritative registration.
            return await self.get(str(assistant.assistant_id))
        return await self.get(str(assistant.assistant_id))

    async def get(self, assistant_id: str) -> Assistant:
        async with self._sessions() as session:
            row = await session.get(AssistantRow, str(assistant_id))
            version = None
            if row is not None and row.application_id == self._application_id:
                version = (
                    await session.execute(
                        select(AssistantVersionRow).where(
                            AssistantVersionRow.assistant_id == str(assistant_id),
                            AssistantVersionRow.version == row.active_version,
                        )
                    )
                ).scalar_one_or_none()
            if row is None or row.application_id != self._application_id or version is None:
                raise RuntimeFailure(
                    ErrorCode.RESOURCE_NOT_FOUND, f"assistant not found: {assistant_id}"
                )
            metadata = dict(version.metadata_json)
            return Assistant(
                AssistantId.parse(row.id),
                row.graph_id,
                version.version,
                metadata.pop("name", None),
                version.config,
                version.context,
                metadata,
            )

    async def all(self) -> tuple[Assistant, ...]:
        async with self._sessions() as session:
            result = await session.execute(
                select(AssistantRow.id).where(AssistantRow.application_id == self._application_id)
            )
            rows = result.fetchall()
        return tuple([await self.get(str(row[0])) for row in rows])

    async def versions(self, assistant_id: str) -> tuple[Assistant, ...]:
        async with self._sessions() as session:
            result = await session.execute(
                select(AssistantRow, AssistantVersionRow)
                .join(AssistantVersionRow, AssistantVersionRow.assistant_id == AssistantRow.id)
                .where(
                    AssistantRow.id == str(assistant_id),
                    AssistantRow.application_id == self._application_id,
                )
                .order_by(AssistantVersionRow.version.desc())
            )
            rows = result.all()
            if not rows:
                raise RuntimeFailure(
                    ErrorCode.RESOURCE_NOT_FOUND, f"assistant not found: {assistant_id}"
                )
            return tuple(self._to_assistant(row, version) for row, version in rows)

    async def update(self, assistant_id: str, assistant: Assistant) -> Assistant:
        async with self._sessions() as session:
            async with session.begin():
                row = await session.get(AssistantRow, str(assistant_id))
                if row is None or row.application_id != self._application_id:
                    raise RuntimeFailure(
                        ErrorCode.RESOURCE_NOT_FOUND, f"assistant not found: {assistant_id}"
                    )
                result = await session.execute(
                    select(AssistantVersionRow.version)
                    .where(AssistantVersionRow.assistant_id == str(assistant_id))
                    .order_by(AssistantVersionRow.version.desc())
                    .limit(1)
                )
                current = result.scalar_one_or_none() or 0
                version = int(current) + 1
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
                row.graph_id = assistant.graph_id
                row.active_version = version
        return await self.get(str(assistant_id))

    async def set_latest(self, assistant_id: str, version: int) -> Assistant:
        async with self._sessions() as session:
            async with session.begin():
                row = await session.get(AssistantRow, str(assistant_id))
                version_row = await session.get(AssistantVersionRow, f"{assistant_id}:{version}")
                if row is None or row.application_id != self._application_id or version_row is None:
                    raise RuntimeFailure(
                        ErrorCode.RESOURCE_NOT_FOUND,
                        f"assistant version not found: {assistant_id}/{version}",
                    )
                row.active_version = version
        return await self.get(str(assistant_id))

    async def delete(self, assistant_id: str, *, delete_threads: bool = False) -> None:
        del delete_threads
        async with self._sessions() as session:
            async with session.begin():
                row = await session.get(AssistantRow, str(assistant_id))
                if row is None or row.application_id != self._application_id:
                    raise RuntimeFailure(
                        ErrorCode.RESOURCE_NOT_FOUND, f"assistant not found: {assistant_id}"
                    )
                await session.execute(
                    text(
                        "DELETE FROM rt_core.assistant_versions WHERE assistant_id = :assistant_id"
                    ),
                    {"assistant_id": str(assistant_id)},
                )
                await session.delete(row)

    async def count(self, *, graph_id: str | None = None) -> int:
        async with self._sessions() as session:
            query = select(AssistantRow).where(AssistantRow.application_id == self._application_id)
            if graph_id is not None:
                query = query.where(AssistantRow.graph_id == graph_id)
            rows = (await session.execute(query)).scalars().all()
            return len(rows)

    @staticmethod
    def _to_assistant(row: AssistantRow, version: AssistantVersionRow) -> Assistant:
        metadata = dict(version.metadata_json)
        return Assistant(
            AssistantId.parse(row.id),
            row.graph_id,
            version.version,
            metadata.pop("name", None),
            version.config,
            version.context,
            metadata,
        )


class PostgresApplicationConfigRepository:
    def __init__(self, sessions: async_sessionmaker[AsyncSession], environment: str) -> None:
        self._sessions, self._environment = sessions, environment

    async def get_active(self, application_id: str) -> ConfigRevision:
        async with self._sessions() as session:
            result = await session.execute(
                text(
                    "SELECT id, revision_number, config, config_hash, active "
                    "FROM rt_core.application_revisions "
                    "WHERE application_id = :application_id AND active = true "
                    "ORDER BY revision_number DESC LIMIT 1"
                ),
                {"application_id": application_id},
            )
            item = result.mappings().first()
            if item is None:
                raise RuntimeFailure(
                    ErrorCode.RESOURCE_NOT_FOUND, f"active config not found: {application_id}"
                )
            return ConfigRevision(
                application_id,
                int(item["revision_number"]),
                dict(item["config"]),
                str(item["config_hash"]),
                bool(item["active"]),
                ConfigRevisionId.parse(str(item["id"])),
            )

    async def create_revision(self, application_id: str, config: dict[str, Any]) -> ConfigRevision:
        encoded = json.dumps(config, sort_keys=True, separators=(",", ":"))
        config_hash = hashlib.sha256(encoded.encode()).hexdigest()
        async with self._sessions() as session:
            async with session.begin():
                # Serialize revision allocation per application without relying on MAX()+1 races.
                await session.execute(
                    text("SELECT pg_advisory_xact_lock(hashtext(:key))"),
                    {"key": f"application-config:{application_id}"},
                )
                result = await session.execute(
                    text(
                        "SELECT COALESCE(MAX(revision_number), 0) + 1 "
                        "FROM rt_core.application_revisions WHERE application_id = :application_id"
                    ),
                    {"application_id": application_id},
                )
                revision = int(result.scalar_one())
                revision_id = ConfigRevisionId.new()
                await session.execute(
                    text(
                        "INSERT INTO rt_core.application_revisions "
                        "(id, application_id, revision_number, config, config_hash, active) "
                        "VALUES (:id, :application_id, :revision_number, CAST(:config AS jsonb), "
                        ":config_hash, false)"
                    ),
                    {
                        "id": str(revision_id),
                        "application_id": application_id,
                        "revision_number": revision,
                        "config": encoded,
                        "config_hash": config_hash,
                    },
                )
        return ConfigRevision(application_id, revision, config, config_hash, False, revision_id)

    async def activate(self, application_id: str, revision: int) -> ConfigRevision:
        async with self._sessions() as session:
            async with session.begin():
                await session.execute(
                    text(
                        "UPDATE rt_core.application_revisions SET active = false "
                        "WHERE application_id = :application_id"
                    ),
                    {"application_id": application_id},
                )
                result = await session.execute(
                    text(
                        "UPDATE rt_core.application_revisions SET active = true "
                        "WHERE application_id = :application_id AND revision_number = :revision "
                        "RETURNING id, config, config_hash"
                    ),
                    {"application_id": application_id, "revision": revision},
                )
                item = result.mappings().first()
                if item is None:
                    raise RuntimeFailure(ErrorCode.RESOURCE_NOT_FOUND, "config revision not found")
        return ConfigRevision(
            application_id,
            revision,
            dict(item["config"]),
            str(item["config_hash"]),
            True,
            ConfigRevisionId.parse(str(item["id"])),
        )


class PostgresThreadRepository:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        scope: ApplicationScope | None = None,
    ) -> None:
        self._sessions = sessions
        self._scope = scope

    async def create(self, thread: Thread) -> Thread:
        if self._scope is None:
            raise RuntimeError("application scope is required to create a thread")
        async with self._sessions() as session:
            async with session.begin():
                session.add(
                    ThreadRow(
                        id=str(thread.thread_id),
                        workspace_id=str(self._scope.workspace_id),
                        project_id=str(self._scope.project_id),
                        application_id=str(self._scope.application_id),
                        status=thread.status.value,
                        metadata_json=thread.metadata,
                    )
                )
        return thread

    async def get(self, thread_id: str) -> Thread:
        async with self._sessions() as session:
            row = await session.get(ThreadRow, str(thread_id))
            if row is None or (
                self._scope is not None and row.application_id != str(self._scope.application_id)
            ):
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
                if row is None or (
                    self._scope is not None
                    and row.application_id != str(self._scope.application_id)
                ):
                    raise RuntimeFailure(
                        ErrorCode.RESOURCE_NOT_FOUND, f"thread not found: {thread.thread_id}"
                    )
                row.status, row.metadata_json = thread.status.value, thread.metadata
        return thread

    async def delete(self, thread_id: str) -> None:
        async with self._sessions() as session:
            async with session.begin():
                row = await session.get(ThreadRow, str(thread_id))
                if row is None or (
                    self._scope is not None
                    and row.application_id != str(self._scope.application_id)
                ):
                    raise RuntimeFailure(
                        ErrorCode.RESOURCE_NOT_FOUND, f"thread not found: {thread_id}"
                    )
                await session.delete(row)

    async def count(
        self, *, metadata: dict[str, object] | None = None, status: str | None = None
    ) -> int:
        items = await self.search(metadata=metadata, status=status, limit=100000, offset=0)
        return len(items)

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
                select(ThreadRow).order_by(ThreadRow.created_at.desc()).limit(limit).offset(offset)
            )
            if self._scope is not None:
                query = query.where(ThreadRow.application_id == str(self._scope.application_id))
            if status is not None:
                query = query.where(ThreadRow.status == status)
            rows = (await session.execute(query)).scalars().all()
            items = [
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
            ]
            return tuple(items)


def _run(row: RunRow) -> Run:
    scope = ApplicationScope(
        WorkspaceId.parse(row.workspace_id),
        ProjectId.parse(row.project_id),
        ApplicationId.parse(row.application_id),
        RevisionId.parse(row.revision_id),
        DeploymentId.parse(row.deployment_id),
    )
    error = (
        RunError(
            row.error.get("code", "EXECUTION_FAILED"),
            row.error.get("message", ""),
            bool(row.error.get("retryable", False)),
            row.error.get("details", {}),
        )
        if row.error
        else None
    )
    return Run(
        ExecutionIdentity(
            scope,
            AssistantId.parse(row.assistant_id),
            RunId.parse(row.id),
            AttemptId.parse(row.attempt_id),
            ThreadId.parse(row.thread_id) if row.thread_id else None,
        ),
        RunStatus(row.status),
        row.metadata_json,
        row.created_at,
        row.updated_at,
        row.result,
        error,
        ExecutionTarget(row.graph_id, row.assistant_version),
    )


class PostgresRunRepository:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def create(self, run: Run) -> Run:
        i = run.identity
        async with self._sessions() as session:
            async with session.begin():
                session.add(
                    RunRow(
                        id=str(i.run_id),
                        workspace_id=str(i.scope.workspace_id),
                        project_id=str(i.scope.project_id),
                        application_id=str(i.scope.application_id),
                        revision_id=str(i.scope.revision_id),
                        deployment_id=str(i.scope.deployment_id),
                        assistant_id=str(i.assistant_id),
                        assistant_version=run.target.assistant_version,
                        graph_id=run.target.graph_id,
                        thread_id=str(i.thread_id) if i.thread_id else None,
                        attempt_id=str(i.attempt_id),
                        status=run.status.value,
                        metadata_json=run.metadata,
                        result=run.result,
                        error={
                            "code": run.error.code,
                            "message": run.error.message,
                            "retryable": run.error.retryable,
                            "details": run.error.details,
                        }
                        if run.error
                        else None,
                    )
                )
                try:
                    await session.flush()
                except IntegrityError as exc:
                    raise RuntimeFailure(
                        ErrorCode.THREAD_BUSY, "thread already has an active run"
                    ) from exc
        return run

    async def get(self, run_id: str) -> Run:
        async with self._sessions() as session:
            row = await session.get(RunRow, str(run_id))
            if row is None:
                raise RuntimeFailure(ErrorCode.RUN_NOT_FOUND, f"run not found: {run_id}")
            return _run(row)

    async def update(self, run: Run) -> Run:
        async with self._sessions() as session:
            async with session.begin():
                row = await session.get(RunRow, str(run.run_id))
                if row is None:
                    raise RuntimeFailure(ErrorCode.RUN_NOT_FOUND, f"run not found: {run.run_id}")
                row.status, row.result, row.error = (
                    run.status.value,
                    run.result,
                    {
                        "code": run.error.code,
                        "message": run.error.message,
                        "retryable": run.error.retryable,
                        "details": run.error.details,
                    }
                    if run.error
                    else None,
                )
        return run

    async def delete(self, run_id: str) -> None:
        async with self._sessions() as session:
            async with session.begin():
                row = await session.get(RunRow, str(run_id))
                if row is None:
                    raise RuntimeFailure(ErrorCode.RUN_NOT_FOUND, f"run not found: {run_id}")
                await session.delete(row)

    async def active_for_thread(self, thread_id: str) -> Run | None:
        async with self._sessions() as session:
            row = (
                await session.execute(
                    select(RunRow).where(
                        RunRow.thread_id == str(thread_id),
                        RunRow.status.in_(
                            [
                                RunStatus.PENDING.value,
                                RunStatus.RUNNING.value,
                                RunStatus.INTERRUPTED.value,
                            ]
                        ),
                    )
                )
            ).scalar_one_or_none()
            return _run(row) if row else None

    async def latest_for_thread(self, thread_id: str) -> Run | None:
        async with self._sessions() as session:
            row = (
                (
                    await session.execute(
                        select(RunRow)
                        .where(RunRow.thread_id == str(thread_id))
                        .order_by(RunRow.created_at.desc())
                    )
                )
                .scalars()
                .first()
            )
            return _run(row) if row else None

    async def list_for_thread(
        self,
        thread_id: str,
        *,
        limit: int = 10,
        offset: int = 0,
        status: str | None = None,
    ) -> tuple[Run, ...]:
        async with self._sessions() as session:
            query = (
                select(RunRow)
                .where(RunRow.thread_id == str(thread_id))
                .order_by(RunRow.created_at.desc())
                .limit(limit)
                .offset(offset)
            )
            if status is not None:
                query = query.where(RunRow.status == status)
            rows = (await session.execute(query)).scalars().all()
            return tuple(_run(row) for row in rows)
