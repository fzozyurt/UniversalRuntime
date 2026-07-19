from __future__ import annotations

import asyncio
import os
import signal
from datetime import UTC, datetime
from typing import Any

import grpc
from google.protobuf import struct_pb2

from universal_runtime.adapters.grpc.generated.runtime.v1 import execution_pb2, execution_pb2_grpc
from universal_runtime.adapters.grpc.payloads import python_to_value, value_to_python
from universal_runtime.adapters.kafka import AioKafkaRunCommandQueue, TopicNames
from universal_runtime.adapters.postgres.database import create_engine, create_session_factory
from universal_runtime.adapters.postgres.events import PostgresEventJournal
from universal_runtime.adapters.postgres.repositories import (
    PostgresRunRepository,
    PostgresThreadRepository,
)
from universal_runtime.bootstrap.runtime_config import LauncherConfig
from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.domain.events import RuntimeEventDraft, RuntimeEventType
from universal_runtime.domain.execution import RunError, RunStatus
from universal_runtime.domain.identity import ExecutionIdentity


def _struct(values: dict[str, Any]) -> struct_pb2.Struct:
    result = struct_pb2.Struct()
    result.update(values)
    return result


def _identity(identity: ExecutionIdentity) -> execution_pb2.ExecutionIdentity:
    return execution_pb2.ExecutionIdentity(
        workspace_id=str(identity.workspace_id),
        project_id=str(identity.project_id),
        application_id=str(identity.application_id),
        revision_id=str(identity.revision_id),
        deployment_id=str(identity.deployment_id),
        assistant_id=str(identity.assistant_id),
        thread_id=str(identity.thread_id) if identity.thread_id else "",
        run_id=str(identity.run_id),
        attempt_id=str(identity.attempt_id),
    )


def _invocation(command: Any) -> execution_pb2.InvokeRequest:
    request = command.request
    return execution_pb2.InvokeRequest(
        identity=_identity(command.identity),
        target=execution_pb2.ExecutionTarget(
            graph_id=request.target.graph_id,
            assistant_version=request.target.assistant_version,
        ),
        input=python_to_value(request.input),
        command=python_to_value(request.command),
        config=_struct(request.config),
        context=_struct(request.context),
        metadata=_struct(request.metadata),
        stream_modes=list(request.stream_modes),
        stream_subgraphs=request.stream_subgraphs,
        priority=int(request.priority),
        timeout_seconds=request.timeout_seconds,
    )


class Dispatcher:
    def __init__(self) -> None:
        self.config = LauncherConfig.from_environment()
        self.engine = create_engine(
            os.environ["UR_DATABASE_URL"],
            pool_size=int(os.environ.get("UR_DISPATCHER_DB_POOL_SIZE", "5")),
            max_overflow=int(os.environ.get("UR_DISPATCHER_DB_MAX_OVERFLOW", "5")),
        )
        sessions = create_session_factory(self.engine)
        self.runs = PostgresRunRepository(sessions)
        self.threads = PostgresThreadRepository(sessions)
        self.events = PostgresEventJournal(sessions)
        topics = TopicNames.from_config(
            prefix=self.config.topic_prefix, environment=self.config.kafka_environment
        )
        self.queue = AioKafkaRunCommandQueue(
            bootstrap_servers=self.config.kafka_bootstrap_servers,
            topics=topics,
            group_id=f"{os.environ.get('UR_APPLICATION_ID', 'default')}.dispatcher",
        )
        targets = [
            target.strip()
            for target in os.environ.get("UR_WORKER_TARGETS", "worker-1:9090,worker-2:9090").split(",")
            if target.strip()
        ]
        if not targets:
            raise RuntimeError("UR_WORKER_TARGETS must contain at least one gRPC target")
        self.channels = [grpc.aio.insecure_channel(target) for target in targets]
        self._worker_index = 0

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            receipt = await self.queue.receive(self._dispatcher_id())
            await self._dispatch(receipt)

    def _dispatcher_id(self) -> Any:
        from universal_runtime.domain.identity import WorkerId

        return WorkerId.parse(os.environ.get("UR_INSTANCE_ID", "dispatcher"))

    async def _dispatch(self, receipt: Any) -> None:
        try:
            run = await self.runs.get(str(receipt.identity.run_id))
        except RuntimeFailure as exc:
            if exc.code is ErrorCode.RUN_NOT_FOUND:
                await self.queue.acknowledge(receipt)
                return
            raise
        if run.status is not RunStatus.PENDING:
            await self.queue.acknowledge(receipt)
            return
        await self.runs.update(run.mark_running(datetime.now(UTC)))
        channel = self.channels[self._worker_index % len(self.channels)]
        self._worker_index += 1
        stub = execution_pb2_grpc.ExecutionServiceStub(channel)
        terminal = False
        last_result: Any = None
        try:
            async for event in stub.Stream(
                execution_pb2.StreamRequest(invocation=_invocation(receipt.command))
            ):
                draft = RuntimeEventDraft(
                    receipt.identity,
                    RuntimeEventType(event.type),
                    tuple(event.namespace),
                    value_to_python(event.data),
                    {
                        key: value_to_python(value)
                        for key, value in event.native.fields.items()
                    },
                )
                await self.events.append(draft)
                if event.type == "state.values":
                    last_result = value_to_python(event.data)
                if event.type in {
                    "run.completed",
                    "run.failed",
                    "run.cancelled",
                    "run.interrupted",
                }:
                    terminal = True
                    current = await self.runs.get(str(run.run_id))
                    if event.type == "run.completed":
                        await self.runs.update(current.complete(last_result, datetime.now(UTC)))
                    elif event.type == "run.interrupted":
                        await self.runs.update(current.mark_interrupted(datetime.now(UTC)))
                    elif event.type == "run.cancelled":
                        await self.runs.update(current.cancel(datetime.now(UTC)))
                    else:
                        await self.runs.update(
                            current.fail(
                                RunError("FRAMEWORK_EXECUTION_FAILED", str(event.data)),
                                datetime.now(UTC),
                            )
                        )
            if not terminal:
                current = await self.runs.get(str(run.run_id))
                await self.runs.update(current.complete(last_result, datetime.now(UTC)))
            if run.thread_id:
                thread = await self.threads.get(str(run.thread_id))
                await self.threads.update(thread.mark_idle(datetime.now(UTC)))
            await self.queue.acknowledge(receipt)
        except Exception as exc:
            current = await self.runs.get(str(run.run_id))
            await self.events.append(
                RuntimeEventDraft(
                    receipt.identity,
                    RuntimeEventType.RUN_FAILED,
                    data={"error": str(exc)},
                )
            )
            if current.status not in {
                RunStatus.SUCCESS,
                RunStatus.ERROR,
                RunStatus.TIMEOUT,
                RunStatus.CANCELLED,
                RunStatus.INTERRUPTED,
            }:
                await self.runs.update(
                    current.fail(RunError("DISPATCH_FAILED", str(exc)), datetime.now(UTC))
                )
            await self.queue.acknowledge(receipt)

    async def close(self) -> None:
        await self.queue.close()
        for channel in self.channels:
            await channel.close()
        await self.engine.dispose()


async def _serve() -> None:
    dispatcher = Dispatcher()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, stop.set)
    loop.add_signal_handler(signal.SIGINT, stop.set)
    try:
        await dispatcher.run(stop)
    finally:
        await dispatcher.close()


def create_dispatch_queue() -> AioKafkaRunCommandQueue:
    config = LauncherConfig.from_environment()
    return AioKafkaRunCommandQueue(
        bootstrap_servers=config.kafka_bootstrap_servers,
        topics=TopicNames.from_config(
            prefix=config.topic_prefix, environment=config.kafka_environment
        ),
        group_id=f"{os.environ.get('UR_APPLICATION_ID', 'default')}.dispatcher",
    )


def main(*, run_forever: bool = False) -> int:
    config = LauncherConfig.from_environment()
    config.validate()
    if run_forever:
        asyncio.run(_serve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main(run_forever=True))
