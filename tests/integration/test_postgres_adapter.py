from __future__ import annotations

import os
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from universal_runtime.adapters.postgres.database import (
    _is_safe_search_path,
    create_engine,
    create_session_factory,
)
from universal_runtime.adapters.postgres.migration import migrate_platform
from universal_runtime.adapters.postgres.models import (
    OutboxEventRow,
    PlatformBase,
    RunRow,
    RuntimeEventBatchRow,
    ThreadRow,
)
from universal_runtime.adapters.postgres.schema import SchemaNames


def test_schema_names_are_scoped_and_validated() -> None:
    schemas = SchemaNames(prefix="test_rt")
    assert schemas.core == "test_rt_core"
    assert schemas.execution == "test_rt_exec"
    assert (
        schemas.application("workspace", "application", "local")
        == "test_rt_a_workspace_application_local"
    )
    assert (
        schemas.framework_state("workspace", "application", "local")
        == "test_rt_s_workspace_application_local"
    )
    with pytest.raises(ValueError):
        schemas.application("bad-key", "application", "local")


def test_fixed_search_path_accepts_only_identifiers() -> None:
    assert _is_safe_search_path(
        "rt_a_workspace_application_local,public"
    )
    assert not _is_safe_search_path(
        "rt_a_workspace;drop schema,public"
    )
    assert not _is_safe_search_path("rt_a_workspace, public")


def test_langgraph_database_url_is_pinned_to_framework_schema() -> None:
    from universal_runtime.adapters.postgres.langgraph import (
        database_url_for_search_path,
    )

    url = database_url_for_search_path(
        "postgresql+psycopg://runtime:secret@localhost/runtime",
        "rt_s_workspace_app_local",
    )
    assert "search_path" in url
    assert "rt_s_workspace_app_local" in url
    assert "public" in url


def test_platform_metadata_contains_contract_tables() -> None:
    tables = PlatformBase.metadata.tables
    assert "rt_core.applications" in tables
    assert "rt_exec.runs" in tables
    assert "rt_exec.outbox_events" in tables
    assert "rt_exec.inbox_events" in tables
    assert "rt_exec.runtime_event_batches" in tables
    assert "rt_exec.worker_leases" in tables


@pytest.mark.asyncio
async def test_framework_state_engine_is_application_scoped() -> None:
    from universal_runtime.adapters.postgres.database import (
        create_framework_state_engine,
    )

    engine = create_framework_state_engine(
        "postgresql+psycopg://runtime:runtime@localhost/runtime",
        workspace_key="workspace",
        application_key="application",
        environment="local",
    )
    await engine.dispose()


POSTGRES_URL = os.getenv("UR_POSTGRES_URL")
pytestmark = pytest.mark.postgres


@pytest.mark.skipif(
    POSTGRES_URL is None,
    reason="UR_POSTGRES_URL is not configured",
)
@pytest.mark.asyncio
async def test_postgres_restart_state_and_idempotency() -> None:
    assert POSTGRES_URL is not None
    engine = create_engine(POSTGRES_URL)
    application_id = f"integration-{uuid4().hex}"
    environment = "test"
    try:
        await migrate_platform(
            engine,
            application_id=application_id,
            environment=environment,
        )
        sessions = create_session_factory(engine)
        now = datetime.now(UTC)
        async with sessions.begin() as session:
            session.add(
                ThreadRow(
                    id="thread-1",
                    workspace_id="workspace",
                    project_id="project",
                    application_id=application_id,
                    status="idle",
                    metadata_json={},
                )
            )
            session.add(
                RunRow(
                    id="run-1",
                    workspace_id="workspace",
                    project_id="project",
                    application_id=application_id,
                    revision_id="revision",
                    deployment_id="deployment",
                    assistant_id="assistant",
                    assistant_version=1,
                    graph_id="graph",
                    thread_id="thread-1",
                    attempt_id="attempt",
                    status="success",
                    metadata_json={},
                    result={"ok": True},
                    created_at=now,
                    updated_at=now,
                )
            )
            session.add(
                RuntimeEventBatchRow(
                    id="batch-1",
                    run_id="run-1",
                    batch_sequence=0,
                    first_sequence=0,
                    last_sequence=0,
                    events=[{"type": "run.completed"}],
                )
            )
            session.add(
                OutboxEventRow(
                    id="outbox-1",
                    event_id="event-1",
                    aggregate_type="run",
                    aggregate_id="run-1",
                    topic="runs",
                    idempotency_key="run-1-completed",
                    payload={"status": "success"},
                )
            )
        await engine.dispose()
        engine = create_engine(POSTGRES_URL)
        sessions = create_session_factory(engine)
        async with sessions() as session:
            run = (
                await session.execute(
                    select(RunRow).where(RunRow.id == "run-1")
                )
            ).scalar_one()
            assert run.workspace_id == "workspace"
            assert run.attempt_id == "attempt"
            assert run.graph_id == "graph"
            assert run.assistant_version == 1
            assert run.result == {"ok": True}
            assert (
                await session.execute(select(RuntimeEventBatchRow))
            ).scalar_one().last_sequence == 0
            duplicate = OutboxEventRow(
                id="outbox-2",
                event_id="event-1",
                aggregate_type="run",
                aggregate_id="run-1",
                topic="runs",
                idempotency_key="run-1-completed-2",
                payload={},
            )
            session.add(duplicate)
            with pytest.raises(IntegrityError):
                await session.flush()
    finally:
        await engine.dispose()


@pytest.mark.skipif(
    POSTGRES_URL is None,
    reason="UR_POSTGRES_URL is not configured",
)
@pytest.mark.asyncio
async def test_postgres_migration_lock_serializes_concurrent_migrations() -> None:
    assert POSTGRES_URL is not None
    import asyncio

    from universal_runtime.adapters.postgres.locks import advisory_migration_lock

    engine = create_engine(POSTGRES_URL, pool_size=2, max_overflow=0)
    order: list[str] = []

    async def holder() -> None:
        async with advisory_migration_lock(
            engine,
            "lock-app",
            "test",
            "platform",
        ):
            order.append("holder-enter")
            await asyncio.sleep(0.15)
            order.append("holder-exit")

    async def waiter() -> None:
        await asyncio.sleep(0.02)
        async with advisory_migration_lock(
            engine,
            "lock-app",
            "test",
            "platform",
        ):
            order.append("waiter-enter")

    try:
        await asyncio.gather(holder(), waiter())
        assert order == [
            "holder-enter",
            "holder-exit",
            "waiter-enter",
        ]
    finally:
        await engine.dispose()


@pytest.mark.skipif(
    POSTGRES_URL is None,
    reason="UR_POSTGRES_URL is not configured",
)
@pytest.mark.asyncio
async def test_postgres_application_schema_isolation() -> None:
    assert POSTGRES_URL is not None
    from sqlalchemy import text

    from universal_runtime.adapters.postgres.database import (
        create_application_engine,
    )

    prefix = f"it{uuid4().hex[:12]}"
    schemas = SchemaNames(prefix=prefix)
    admin = create_engine(POSTGRES_URL)
    first = create_application_engine(
        POSTGRES_URL,
        workspace_key="workspace",
        application_key="one",
        environment="test",
        schemas=schemas,
    )
    second = create_application_engine(
        POSTGRES_URL,
        workspace_key="workspace",
        application_key="two",
        environment="test",
        schemas=schemas,
    )
    try:
        async with admin.begin() as connection:
            await connection.execute(
                text(
                    f'CREATE SCHEMA "{schemas.application("workspace", "one", "test")}"'
                )
            )
            await connection.execute(
                text(
                    f'CREATE SCHEMA "{schemas.application("workspace", "two", "test")}"'
                )
            )
        async with first.begin() as connection:
            await connection.execute(
                text("CREATE TABLE scoped_values (value text NOT NULL)")
            )
            await connection.execute(
                text("INSERT INTO scoped_values VALUES ('one')")
            )
        async with second.begin() as connection:
            await connection.execute(
                text("CREATE TABLE scoped_values (value text NOT NULL)")
            )
            await connection.execute(
                text("INSERT INTO scoped_values VALUES ('two')")
            )
        async with first.connect() as connection:
            assert (
                await connection.execute(
                    text("SELECT value FROM scoped_values")
                )
            ).scalar_one() == "one"
        async with second.connect() as connection:
            assert (
                await connection.execute(
                    text("SELECT value FROM scoped_values")
                )
            ).scalar_one() == "two"
    finally:
        await first.dispose()
        await second.dispose()
        async with admin.begin() as connection:
            await connection.execute(
                text(
                    f'DROP SCHEMA IF EXISTS "{schemas.application("workspace", "one", "test")}" CASCADE'
                )
            )
            await connection.execute(
                text(
                    f'DROP SCHEMA IF EXISTS "{schemas.application("workspace", "two", "test")}" CASCADE'
                )
            )
        await admin.dispose()


@pytest.mark.skipif(
    POSTGRES_URL is None,
    reason="UR_POSTGRES_URL is not configured",
)
@pytest.mark.asyncio
async def test_postgres_langgraph_state_survives_provider_restart() -> None:
    assert POSTGRES_URL is not None
    from langgraph.graph import END, START, StateGraph

    from universal_runtime.adapters.postgres.langgraph import (
        managed_langgraph_persistence,
    )

    migration_engine = create_engine(
        POSTGRES_URL,
        pool_size=2,
        max_overflow=0,
    )
    application_id = f"langgraph-{uuid4().hex}"
    config = {
        "configurable": {"thread_id": f"thread-{uuid4().hex}"}
    }

    def increment(state: dict[str, int]) -> dict[str, int]:
        return {"count": state.get("count", 0) + 1}

    try:
        async with managed_langgraph_persistence(
            POSTGRES_URL,
            migration_engine=migration_engine,
            application_id=application_id,
            environment="test",
            workspace_key="workspace",
            application_key="langgraph_app",
        ) as persistence:
            builder = StateGraph(dict)
            builder.add_node("increment", increment)
            builder.add_edge(START, "increment")
            builder.add_edge("increment", END)
            graph = builder.compile(
                checkpointer=persistence.checkpointer,
                store=persistence.store,
            )
            result = await graph.ainvoke({"count": 0}, config)
            assert result["count"] == 1
            assert (await graph.aget_state(config)).values["count"] == 1
            assert (
                len(
                    [
                        item
                        async for item in graph.aget_state_history(config)
                    ]
                )
                >= 1
            )

        async with managed_langgraph_persistence(
            POSTGRES_URL,
            migration_engine=migration_engine,
            application_id=application_id,
            environment="test",
            workspace_key="workspace",
            application_key="langgraph_app",
        ) as persistence:
            builder = StateGraph(dict)
            builder.add_node("increment", increment)
            builder.add_edge(START, "increment")
            builder.add_edge("increment", END)
            graph = builder.compile(
                checkpointer=persistence.checkpointer,
                store=persistence.store,
            )
            assert (await graph.aget_state(config)).values["count"] == 1
    finally:
        await migration_engine.dispose()
