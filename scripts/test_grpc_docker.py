from __future__ import annotations

import asyncio
import os
import subprocess
import time
from collections.abc import Awaitable, Callable

import grpc
from grpc_health.v1 import health_pb2, health_pb2_grpc

from universal_runtime.adapters.grpc.generated.runtime.v1 import (
    execution_pb2,
    execution_pb2_grpc,
    worker_pb2,
    worker_pb2_grpc,
)

IMAGE = os.environ.get("UR_GRPC_DOCKER_IMAGE", "universal-runtime:grpc-test")
CONTAINER = "ur-grpc-e2e"
HOST = "127.0.0.1"
PORT = 19090


def docker(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603
        ["docker", *args],  # noqa: S607
        check=check,
        text=True,
        capture_output=True,
    )


def identity(run_id: str, thread_id: str) -> execution_pb2.ExecutionIdentity:
    return execution_pb2.ExecutionIdentity(
        workspace_id="workspace",
        project_id="project",
        application_id="application",
        revision_id="revision",
        deployment_id="deployment",
        assistant_id="assistant",
        thread_id=thread_id,
        run_id=run_id,
        attempt_id=f"attempt-{run_id}",
    )


async def eventually[T](operation: Callable[[], Awaitable[T]], deadline_seconds: float = 15) -> T:
    deadline = time.monotonic() + deadline_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            return await operation()
        except (grpc.aio.AioRpcError, OSError) as exc:
            last_error = exc
            await asyncio.sleep(0.2)
    raise AssertionError("gRPC endpoint did not become ready") from last_error


async def run_checks() -> None:
    channel = grpc.aio.insecure_channel(f"{HOST}:{PORT}")
    try:
        health = health_pb2_grpc.HealthStub(channel)
        response = await eventually(lambda: health.Check(health_pb2.HealthCheckRequest(service="")))
        assert response.status == health_pb2.HealthCheckResponse.SERVING

        control = worker_pb2_grpc.WorkerControlServiceStub(channel)
        execution = execution_pb2_grpc.ExecutionServiceStub(channel)
        register = await control.Register(
            worker_pb2.RegisterWorkerRequest(
                worker_id="docker-worker",
                application_id="application",
                revision_id="revision",
                deployment_id="deployment",
                config_hash="config-hash",
                max_concurrency=8,
            )
        )
        assert register.accepted

        async def invoke(index: int) -> str:
            result = await execution.Invoke(
                execution_pb2.InvokeRequest(
                    identity=identity(f"invoke-{index}", f"thread-invoke-{index}")
                )
            )
            return result.run_id

        assert sorted(await asyncio.gather(*(invoke(index) for index in range(20)))) == sorted(
            f"invoke-{index}" for index in range(20)
        )

        async def stream(index: int) -> tuple[str, str]:
            call = execution.Stream(
                execution_pb2.StreamRequest(
                    invocation=execution_pb2.InvokeRequest(
                        identity=identity(f"stream-{index}", f"thread-stream-{index}"),
                        stream_modes=["values"],
                    )
                )
            )
            event = await call.read()
            assert event.type == "run.started"
            return event.identity.run_id, event.identity.thread_id

        assert sorted(await asyncio.gather(*(stream(index) for index in range(20)))) == sorted(
            (f"stream-{index}", f"thread-stream-{index}") for index in range(20)
        )

        active_identity = identity("active-cancel", "thread-active")
        active_call = execution.Stream(
            execution_pb2.StreamRequest(
                invocation=execution_pb2.InvokeRequest(identity=active_identity)
            )
        )
        first = await active_call.read()
        assert first.type == "run.started"
        await execution.Cancel(
            execution_pb2.CancelRequest(identity=active_identity, action="interrupt")
        )
        assert await active_call.read() == grpc.aio.EOF

        async def work_messages() -> list[worker_pb2.ControllerMessage]:
            request_queue: asyncio.Queue[worker_pb2.WorkerMessage | None] = asyncio.Queue()
            await request_queue.put(
                worker_pb2.WorkerMessage(
                    heartbeat=worker_pb2.WorkerHeartbeat(worker_id="docker-worker")
                )
            )
            await request_queue.put(None)

            async def requests():
                while True:
                    request = await request_queue.get()
                    if request is None:
                        return
                    yield request

            return [message async for message in control.Work(requests())]

        messages = await work_messages()
        assert messages[0].HasField("config_update")
        assert messages[0].config_update.fields["worker_id"].string_value == "docker-worker"

        await control.Drain(
            worker_pb2.DrainWorkerRequest(worker_id="docker-worker", timeout_seconds=5)
        )
    finally:
        await channel.close()


def main() -> int:
    docker("rm", "-f", CONTAINER, check=False)
    docker(
        "run",
        "-d",
        "--name",
        CONTAINER,
        "-p",
        f"{PORT}:9090",
        "-e",
        "UR_MODE=worker",
        "-e",
        "UR_GRPC_HOST=0.0.0.0",
        "-e",
        "UR_GRPC_PORT=9090",
        "-e",
        "UR_WORKER_MAX_CONCURRENCY=8",
        IMAGE,
    )
    try:
        asyncio.run(run_checks())
        docker("stop", "-t", "10", CONTAINER)
        result = docker("inspect", "-f", "{{.State.ExitCode}}", CONTAINER)
        assert result.stdout.strip() == "0", result.stdout
        print(
            "gRPC Docker E2E: PASS (health/register, 20 invoke, 20 streams, active cancel, work, drain)"
        )
        return 0
    finally:
        docker("rm", "-f", CONTAINER, check=False)


if __name__ == "__main__":
    raise SystemExit(main())
