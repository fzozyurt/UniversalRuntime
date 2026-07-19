from __future__ import annotations

from dataclasses import dataclass

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc

from universal_runtime.adapters.grpc.generated.runtime.v1 import execution_pb2_grpc, worker_pb2_grpc

from .worker import BoundedWorker, ExecutionServicer, WorkerConfig, WorkerControlServicer


@dataclass(slots=True)
class WorkerServer:
    worker: BoundedWorker
    server: grpc.aio.Server

    @classmethod
    def create(cls, *, configured_concurrency: int, policy_ceiling: int) -> WorkerServer:
        worker = BoundedWorker(WorkerConfig(configured_concurrency, policy_ceiling))
        server = grpc.aio.server()
        worker_pb2_grpc.add_WorkerControlServiceServicer_to_server(
            WorkerControlServicer(worker), server
        )
        execution_pb2_grpc.add_ExecutionServiceServicer_to_server(ExecutionServicer(worker), server)
        health_servicer = health.HealthServicer()
        health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)
        health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
        return cls(worker, server)

    async def start(self, host: str, port: int) -> None:
        self.server.add_insecure_port(f"{host}:{port}")
        await self.server.start()
        await self.server.wait_for_termination()

    async def stop(self, grace: float) -> None:
        await self.worker.drain(grace)
        await self.server.stop(grace)
