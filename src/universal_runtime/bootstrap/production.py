from __future__ import annotations

import os
from typing import Any, cast

from universal_runtime.adapters.kafka import AioKafkaRunCommandQueue, TopicNames
from universal_runtime.adapters.memory.capacity import ExecutionCapacity
from universal_runtime.adapters.memory.registry import InMemoryAdapterRegistry
from universal_runtime.adapters.postgres.database import create_engine, create_session_factory
from universal_runtime.adapters.postgres.events import PostgresEventJournal
from universal_runtime.adapters.postgres.repositories import (
    PostgresApplicationConfigRepository,
    PostgresAssistantRepository,
    PostgresRunRepository,
    PostgresThreadRepository,
)
from universal_runtime.application.runtime_service import RuntimeExecutionService
from universal_runtime.bootstrap.local import LocalRuntime
from universal_runtime.domain.identity import (
    ApplicationId,
    ApplicationScope,
    DeploymentId,
    ProjectId,
    RevisionId,
    WorkspaceId,
)


def _application_scope() -> ApplicationScope:
    return ApplicationScope(
        WorkspaceId.parse(os.environ.get("UR_WORKSPACE_ID", "default")),
        ProjectId.parse(os.environ.get("UR_PROJECT_ID", "default")),
        ApplicationId.parse(os.environ.get("UR_APPLICATION_ID", "default")),
        RevisionId.parse(os.environ.get("UR_REVISION_ID", "active")),
        DeploymentId.parse(os.environ.get("UR_DEPLOYMENT_ID", "local")),
    )


def create_production_runtime() -> LocalRuntime:
    database_url = os.environ.get("UR_DATABASE_URL")
    if not database_url:
        raise RuntimeError("UR_DATABASE_URL is required in production profile")
    engine = create_engine(
        database_url,
        pool_size=int(os.environ.get("UR_DB_POOL_SIZE", "10")),
        max_overflow=int(os.environ.get("UR_DB_MAX_OVERFLOW", "20")),
    )
    sessions = create_session_factory(engine)
    scope = _application_scope()
    application_id = str(scope.application_id)
    assistants = PostgresAssistantRepository(sessions, application_id)
    threads = PostgresThreadRepository(sessions, scope)
    runs = PostgresRunRepository(sessions)
    events = PostgresEventJournal(sessions)
    commands = AioKafkaRunCommandQueue(
        bootstrap_servers=os.environ.get("UR_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        topics=TopicNames.from_config(
            prefix=os.environ.get("UR_TOPIC_PREFIX", "rt"),
            environment=os.environ.get("UR_KAFKA_ENVIRONMENT", "local"),
        ),
        group_id=f"{application_id}.gateway",
    )
    capacity = ExecutionCapacity(int(os.environ.get("UR_WORKER_MAX_CONCURRENCY", "8")))
    adapters = InMemoryAdapterRegistry()
    return LocalRuntime(
        config=PostgresApplicationConfigRepository(
            sessions, os.environ.get("UR_KAFKA_ENVIRONMENT", "local")
        ),
        assistants=cast(Any, assistants),
        outbox=None,
        threads=cast(Any, threads),
        runs=cast(Any, runs),
        events=cast(Any, events),
        commands=commands,
        adapters=adapters,
        capacity=capacity,
        execution=RuntimeExecutionService(
            threads=threads,
            runs=runs,
            commands=commands,
            journal=events,
            replay=events,
            subscription=events,
            assistants=assistants,
            adapters=adapters,
            capacity=capacity,
        ),
        execute_locally=False,
    )
