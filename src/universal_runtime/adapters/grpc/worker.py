# ruff: noqa: N802,ASYNC109,ASYNC110
from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime

from google.protobuf import empty_pb2, struct_pb2

from universal_runtime.adapters.grpc.generated.runtime.v1 import (
    execution_pb2,
    execution_pb2_grpc,
    worker_pb2,
    worker_pb2_grpc,
)
from universal_runtime.domain.execution import ExecutionRequest
from universal_runtime.domain.identity import (
    ApplicationId,
    ApplicationScope,
    AssistantId,
    AttemptId,
    DeploymentId,
    ExecutionIdentity,
    ProjectId,
    RevisionId,
    RunId,
    ThreadId,
    WorkspaceId,
)
from universal_runtime.ports.runtime_adapter import RuntimeAdapter
from universal_runtime.telemetry.bootstrap import initialize
from universal_runtime.telemetry.logging import configure_logging
from universal_runtime.telemetry.tracing import record_failure, runtime_run_span

from .payloads import python_to_value, value_to_python


@dataclass(frozen=True, slots=True)
class WorkerConfig:
    configured_concurrency: int
    policy_ceiling: int
    environment_name: str = "UR_WORKER_MAX_CONCURRENCY"

    def resolve(self, environ: dict[str, str] | None = None) -> int:
        values = environ or os.environ
        raw = values.get(self.environment_name)
        env_limit = int(raw) if raw is not None else self.configured_concurrency
        if env_limit < 1:
            raise ValueError("worker concurrency must be positive")
        return min(self.configured_concurrency, env_limit, self.policy_ceiling)


@dataclass(frozen=True, slots=True)
class WorkerRegistration:
    worker_id: str
    application_id: str
    revision_id: str
    deployment_id: str
    config_hash: str
    max_concurrency: int
    capabilities: worker_pb2.WorkerCapabilities


class BoundedWorker:
    def __init__(self, config: WorkerConfig) -> None:
        self.max_concurrency = config.resolve()
        self._slots = asyncio.Semaphore(self.max_concurrency)
        self._active = 0
        self._draining = False
        self.registration: WorkerRegistration | None = None
        self._running: dict[str, asyncio.Task[object]] = {}

    def register(self, request: worker_pb2.RegisterWorkerRequest) -> WorkerRegistration:
        if request.max_concurrency != self.max_concurrency:
            raise ValueError("worker max concurrency does not match resolved policy")
        self.registration = WorkerRegistration(
            request.worker_id,
            request.application_id,
            request.revision_id,
            request.deployment_id,
            request.config_hash,
            request.max_concurrency,
            request.capabilities,
        )
        return self.registration

    def register_running(self, run_id: str) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._running[run_id] = task

    def unregister_running(self, run_id: str) -> None:
        self._running.pop(run_id, None)

    async def cancel(self, run_id: str) -> bool:
        task = self._running.get(run_id)
        if task is None:
            return False
        task.cancel()
        return True

    async def acquire(self) -> None:
        if self._draining:
            raise RuntimeError("worker is draining")
        await self._slots.acquire()
        self._active += 1

    def release(self) -> None:
        if self._active:
            self._active -= 1
            self._slots.release()

    async def drain(self, timeout: float) -> None:
        self._draining = True
        deadline = asyncio.get_running_loop().time() + timeout
        while self._active and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.01)
        if self._active:
            raise TimeoutError("worker drain timed out")

    @property
    def active_executions(self) -> int:
        return self._active

    @property
    def available_slots(self) -> int:
        return self.max_concurrency - self._active


class WorkerControlServicer(worker_pb2_grpc.WorkerControlServiceServicer):
    def __init__(self, worker: BoundedWorker) -> None:
        self.worker = worker

    async def Register(
        self, request: worker_pb2.RegisterWorkerRequest, context: object
    ) -> worker_pb2.RegisterWorkerResponse:
        del context
        try:
            registration = self.worker.register(request)
        except ValueError as exc:
            return worker_pb2.RegisterWorkerResponse(accepted=False, reason=str(exc))
        return worker_pb2.RegisterWorkerResponse(
            accepted=True,
            heartbeat_interval_seconds=15,
            resolved_execution_defaults=struct_pb2.Struct(
                fields={"config_hash": struct_pb2.Value(string_value=registration.config_hash)}
            ),
        )

    async def Work(
        self,
        request_iterator: AsyncIterator[worker_pb2.WorkerMessage],
        context: object,
    ) -> AsyncIterator[worker_pb2.ControllerMessage]:
        del context
        async for message in request_iterator:
            if message.HasField("heartbeat"):
                heartbeat = message.heartbeat
                yield worker_pb2.ControllerMessage(
                    config_update=struct_pb2.Struct(
                        fields={
                            "worker_id": struct_pb2.Value(string_value=heartbeat.worker_id),
                            "available_slots": struct_pb2.Value(
                                number_value=self.worker.available_slots
                            ),
                        }
                    )
                )

    async def Drain(
        self, request: worker_pb2.DrainWorkerRequest, context: object
    ) -> empty_pb2.Empty:
        del context
        if (
            self.worker.registration is None
            or request.worker_id != self.worker.registration.worker_id
        ):
            raise ValueError("unknown worker")
        await self.worker.drain(request.timeout_seconds or 30)
        return empty_pb2.Empty()


class ExecutionServicer(execution_pb2_grpc.ExecutionServiceServicer):
    def __init__(self, worker: BoundedWorker, adapter: RuntimeAdapter | dict[str, RuntimeAdapter] | None = None) -> None:
        self.worker, self.adapter = worker, adapter

    def _adapter_for(self, assistant_id: str) -> RuntimeAdapter:
        if isinstance(self.adapter, dict):
            try:
                return self.adapter[assistant_id]
            except KeyError as exc:
                raise RuntimeError(f"no adapter registered for assistant: {assistant_id}") from exc
        if self.adapter is None:
            raise RuntimeError("worker graph adapter is not configured")
        return self.adapter

    async def Invoke(
        self, request: execution_pb2.InvokeRequest, context: object
    ) -> execution_pb2.InvokeResponse:
        del context
        await self.worker.acquire()
        self.worker.register_running(request.identity.run_id)
        telemetry = initialize(component="worker")
        configure_logging()
        try:
            with runtime_run_span(telemetry.tracer, {"runtime.run_id": request.identity.run_id, "runtime.assistant_id": request.identity.assistant_id}) as current_span:
                try:
                    adapter = self._adapter_for(request.identity.assistant_id)
                    output = await adapter.invoke(_request_from_proto(request))
                    return execution_pb2.InvokeResponse(run_id=request.identity.run_id, status="accepted", output=python_to_value(output))
                except Exception as exc:
                    record_failure(current_span, exc, error_code="RUNTIME_EXECUTION_FAILED")
                    raise
        finally:
            self.worker.unregister_running(request.identity.run_id)
            self.worker.release()

    async def Stream(
        self, request: execution_pb2.StreamRequest, context: object
    ) -> AsyncIterator[execution_pb2.RuntimeEvent]:
        del context
        await self.worker.acquire()
        self.worker.register_running(request.invocation.identity.run_id)
        try:
            adapter = self._adapter_for(request.invocation.identity.assistant_id)
            invocation = _request_from_proto(request.invocation)
            sequence = 0
            async for draft in adapter.stream(invocation):
                event = execution_pb2.RuntimeEvent(
                    type=str(draft.type), sequence=sequence, namespace=list(draft.namespace)
                )
                event.identity.CopyFrom(request.invocation.identity)
                event.timestamp.FromDatetime(datetime.now(UTC))
                event.data.CopyFrom(python_to_value(draft.data))
                event.native.update(draft.native)
                yield event
                sequence += 1
        finally:
            self.worker.unregister_running(request.invocation.identity.run_id)
            self.worker.release()

    async def Cancel(
        self, request: execution_pb2.CancelRequest, context: object
    ) -> empty_pb2.Empty:
        del context
        await self.worker.cancel(request.identity.run_id)
        return empty_pb2.Empty()


def _request_from_proto(request: execution_pb2.InvokeRequest) -> ExecutionRequest:
    raw = request.identity
    identity = ExecutionIdentity(
        ApplicationScope(
            WorkspaceId.parse(raw.workspace_id),
            ProjectId.parse(raw.project_id),
            ApplicationId.parse(raw.application_id),
            RevisionId.parse(raw.revision_id),
            DeploymentId.parse(raw.deployment_id),
        ),
        AssistantId.parse(raw.assistant_id),
        RunId.parse(raw.run_id),
        AttemptId.parse(raw.attempt_id),
        ThreadId.parse(raw.thread_id) if raw.thread_id else None,
    )
    return ExecutionRequest(
        identity=identity,
        input=value_to_python(request.input),
        command=value_to_python(request.command),
        config={key: value_to_python(value) for key, value in request.config.fields.items()},
        context={key: value_to_python(value) for key, value in request.context.fields.items()},
        metadata={key: value_to_python(value) for key, value in request.metadata.fields.items()},
        stream_modes=tuple(request.stream_modes or ("values",)),
        stream_subgraphs=request.stream_subgraphs,
    )
