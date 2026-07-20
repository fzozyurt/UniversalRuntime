from __future__ import annotations

from datetime import UTC, datetime

import grpc

from universal_runtime.adapters.grpc.generated.runtime.v1 import (
    execution_pb2,
    execution_pb2_grpc,
)
from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.domain.execution import Run
from universal_runtime.ports.cancellation import RunCancellation
from universal_runtime.ports.workers import WorkerRegistry


class LeasedGrpcRunCancellation(RunCancellation):
    def __init__(self, workers: WorkerRegistry, *, timeout_seconds: float = 3.0) -> None:
        self._workers = workers
        self._timeout_seconds = timeout_seconds

    async def cancel(self, run: Run) -> bool:
        lease = await self._workers.active_lease(run.run_id, now=datetime.now(UTC))
        if lease is None:
            return False
        identity = run.identity
        request = execution_pb2.CancelRequest(
            identity=execution_pb2.ExecutionIdentity(
                workspace_id=str(identity.workspace_id),
                project_id=str(identity.project_id),
                application_id=str(identity.application_id),
                revision_id=str(identity.revision_id),
                deployment_id=str(identity.deployment_id),
                assistant_id=str(identity.assistant_id),
                thread_id=str(identity.thread_id) if identity.thread_id else "",
                run_id=str(identity.run_id),
                attempt_id=str(identity.attempt_id),
            ),
            action="cancel",
        )
        channel = grpc.aio.insecure_channel(lease.grpc_target)
        try:
            stub = execution_pb2_grpc.ExecutionServiceStub(channel)
            await stub.Cancel(request, timeout=self._timeout_seconds)
            return True
        except grpc.aio.AioRpcError as exc:
            raise RuntimeFailure(
                ErrorCode.INFRASTRUCTURE_UNAVAILABLE,
                "active worker did not accept run cancellation",
                retryable=True,
                details={
                    "run_id": str(run.run_id),
                    "worker_id": str(lease.worker_id),
                    "grpc_status": exc.code().name,
                },
            ) from exc
        finally:
            await channel.close()
