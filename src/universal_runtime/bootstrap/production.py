from __future__ import annotations

import os
from typing import Any, cast

from universal_runtime.adapters.kafka import AioKafkaRunCommandQueue, TopicNames
from universal_runtime.adapters.memory.capacity import ExecutionCapacity
from universal_runtime.adapters.memory.registry import InMemoryAdapterRegistry
from universal_runtime.adapters.postgres.assistants import PostgresGatewayAssistantRepository
from universal_runtime.adapters.postgres.control_plane import PostgresControlPlaneCatalog
from universal_runtime.adapters.postgres.database import create_engine, create_session_factory
from universal_runtime.adapters.postgres.events import PostgresEventJournal
from universal_runtime.adapters.postgres.repositories import (
    PostgresApplicationConfigRepository,
    PostgresAssistantRepository,
    PostgresRunRepository,
    PostgresThreadRepository,
)
from universal_runtime.adapters.postgres.threads import (
    PostgresThreadApplicationBinder,
    SharedPostgresThreadRepository,
)
from universal_runtime.application.bound_execution_service import ApplicationBoundExecutionService
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
    database_url = os.environ.get("UR_PLATFORM_DATABASE_URL") or os.environ.get(
        "UR_DATABASE_URL"
    )
    if not database_url:
        raise RuntimeError("UR_PLATFORM_DATABASE_URL or UR_DATABASE_URL is required")
    engine = create_engine(
        database_url,
        pool_size=int(os.environ.get("UR_DB_POOL_SIZE", "10")),
        max_overflow=int(os.environ.get("UR_DB_MAX_OVERFLOW", "20")),
    )
    sessions = create_session_factory(engine)
    scope = _application_scope()
    environment = os.environ.get("UR_KAFKA_ENVIRONMENT", "local")
    shared_gateway = os.environ.get("UR_MODE", "gateway") == "gateway"

    plans: PostgresControlPlaneCatalog | None
    if shared_gateway:
        assistants: Any = PostgresGatewayAssistantRepository(
            sessions,
            workspace_id=scope.workspace_id,
            project_id=scope.project_id,
        )
        threads: Any = SharedPostgresThreadRepository(
            sessions,
            workspace_id=scope.workspace_id,
            project_id=scope.project_id,
        )
        plans = PostgresControlPlaneCatalog(sessions, environment=environment)
        thread_binder = PostgresThreadApplicationBinder(sessions)
    else:
        assistants = PostgresAssistantRepository(sessions, str(scope.application_id))
        threads = PostgresThreadRepository(sessions, scope)
        plans = None
        thread_binder = None

    runs = PostgresRunRepository(sessions)
    events = PostgresEventJournal(sessions)
    commands = AioKafkaRunCommandQueue(
        bootstrap_servers=os.environ.get("UR_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        topics=TopicNames.from_config(
            prefix=os.environ.get("UR_TOPIC_PREFIX", "rt"),
            environment=environment,
        ),
        group_id=(
            os.environ.get("UR_GATEWAY_COMMAND_GROUP_ID", "runtime.gateway")
            if shared_gateway
            else f"{scope.application_id}.standalone"
        ),
    )
    capacity = ExecutionCapacity(int(os.environ.get("UR_WORKER_MAX_CONCURRENCY", "8")))
    adapters = InMemoryAdapterRegistry()
    dependencies: dict[str, Any] = {
        "threads": threads,
        "runs": runs,
        "commands": commands,
        "journal": events,
        "replay": events,
        "subscription": events,
        "assistants": assistants,
        "plan_resolver": plans,
        "execution_scope": scope,
        "adapters": adapters,
        "capacity": capacity,
    }
    execution: RuntimeExecutionService
    if thread_binder is not None:
        execution = ApplicationBoundExecutionService(
            thread_binder=thread_binder,
            **dependencies,
        )
    else:
        execution = RuntimeExecutionService(**dependencies)

    return LocalRuntime(
        config=PostgresApplicationConfigRepository(sessions, environment),
        assistants=cast(Any, assistants),
        outbox=None,
        threads=cast(Any, threads),
        runs=cast(Any, runs),
        events=cast(Any, events),
        commands=commands,
        adapters=adapters,
        capacity=capacity,
        execution=execution,
        execute_locally=False,
    )
