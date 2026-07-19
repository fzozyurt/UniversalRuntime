from __future__ import annotations

import pytest

from universal_runtime.adapters.grpc.generated.runtime.v1 import worker_pb2
from universal_runtime.adapters.grpc.payloads import python_to_value, value_to_python
from universal_runtime.adapters.grpc.worker import BoundedWorker, WorkerConfig


def test_value_roundtrip_scalar_list_object_and_null() -> None:
    values = [None, True, 4, 2.5, "text", [1, None], {"key": "value"}]
    assert [value_to_python(python_to_value(value)) for value in values] == values


def test_worker_concurrency_uses_minimum_of_config_env_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("UR_WORKER_MAX_CONCURRENCY", "3")
    assert WorkerConfig(8, 2).resolve() == 2


@pytest.mark.asyncio
async def test_registration_and_drain() -> None:
    worker = BoundedWorker(WorkerConfig(2, 4))
    request = worker_pb2.RegisterWorkerRequest(worker_id="w", max_concurrency=2, config_hash="hash")
    registration = worker.register(request)
    assert registration.config_hash == "hash"
    await worker.acquire()
    assert worker.available_slots == 1
    worker.release()
    await worker.drain(0.1)
    with pytest.raises(RuntimeError):
        await worker.acquire()


@pytest.mark.asyncio
async def test_cancel_stops_registered_task() -> None:
    worker = BoundedWorker(WorkerConfig(1, 1))
    started = __import__("asyncio").Event()
    release = __import__("asyncio").Event()

    async def running() -> None:
        await worker.acquire()
        worker.register_running("run")
        started.set()
        try:
            await release.wait()
        finally:
            worker.unregister_running("run")
            worker.release()

    task = __import__("asyncio").create_task(running())
    await started.wait()
    assert await worker.cancel("run")
    with pytest.raises(__import__("asyncio").CancelledError):
        await task
