from __future__ import annotations

import os
from typing import Any, cast

from universal_runtime.adapters.kafka import AioKafkaRunCommandQueue
from universal_runtime.adapters.memory.capacity import ExecutionCapacity
from universal_runtime.adapters.memory.registry import InMemoryAdapterRegistry
from universal_runtime.adapters.postgres.database import create_engine, create_session_factory
from universal_runtime.adapters.postgres.repositories import (
    PostgresApplicationConfigRepository,
    PostgresAssistantRepository,
    PostgresRunRepository,
    PostgresThreadRepository,
)
from universal_runtime.application.runtime_service import RuntimeExecutionService
from universal_runtime.bootstrap.local import LocalRuntime


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
    application_id = os.environ.get("UR_APPLICATION_ID", "default")
    environment = os.environ.get("UR_KAFKA_ENVIRONMENT", "local")
    assistants = PostgresAssistantRepository(sessions, application_id)
    threads = PostgresThreadRepository(sessions)
    runs = PostgresRunRepository(sessions)
    commands = AioKafkaRunCommandQueue(
        bootstrap_servers=os.environ.get("UR_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        prefix=os.environ.get("UR_TOPIC_PREFIX", "rt"),
        environment=environment,
        application_id=application_id,
        # Gateway only produces. The group is retained for the queue port but
        # worker replicas use their own shared application consumer group.
        group_id=f"rt.{environment}.{application_id}.gateway.v1",
    )
    capacity = ExecutionCapacity(int(os.environ.get("UR_WORKER_MAX_CONCURRENCY", "8")))
    adapters = InMemoryAdapterRegistry()
    return LocalRuntime(
        config=PostgresApplicationConfigRepository(sessions, environment),
        assistants=cast(Any, assistants),
        outbox=None,
        threads=cast(Any, threads),
        runs=cast(Any, runs),
        events=None,
        commands=commands,
        adapters=adapters,
        capacity=capacity,
        execution=RuntimeExecutionService(
            threads=threads,
            runs=runs,
            commands=commands,
            adapters=adapters,
            capacity=capacity,
        ),
        execute_locally=False,
    )
