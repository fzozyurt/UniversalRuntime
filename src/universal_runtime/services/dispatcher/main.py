from __future__ import annotations

import asyncio
import logging
import os
import signal
from datetime import UTC, datetime, timedelta

import grpc

from universal_runtime.adapters.grpc.execution_client import GrpcExecutionClient
from universal_runtime.adapters.kafka import AioKafkaRunCommandQueue, TopicNames
from universal_runtime.adapters.postgres.database import (
    create_engine,
    create_session_factory,
)
from universal_runtime.adapters.postgres.events import PostgresEventJournal
from universal_runtime.adapters.postgres.repositories import (
    PostgresRunRepository,
    PostgresThreadRepository,
)
from universal_runtime.adapters.postgres.workers import PostgresWorkerRegistry
from universal_runtime.bootstrap.runtime_config import LauncherConfig
from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.domain.events import RuntimeEventDraft, RuntimeEventType
from universal_runtime.domain.execution import (
    Run,
    RunCommandReceipt,
    RunError,
    RunStatus,
)
from universal_runtime.domain.identity import WorkerId
from universal_runtime.domain.primitives.json_types import JsonValue
from universal_runtime.domain.workers import WorkerLease

_LOGGER = logging.getLogger(__name__)
_TERMINAL_RUN_STATUSES = {
    RunStatus.SUCCESS,
    RunStatus.ERROR,
    RunStatus.TIMEOUT,
    RunStatus.CANCELLED,
    RunStatus.INTERRUPTED,
}
_TERMINAL_EVENT_TYPES = {
    RuntimeEventType.RUN_COMPLETED,
    RuntimeEventType.RUN_FAILED,
    RuntimeEventType.RUN_CANCELLED,
    RuntimeEventType.RUN_INTERRUPTED,
    RuntimeEventType.RUN_TIMEOUT,
}
_RETRYABLE_GRPC_CODES = {
    grpc.StatusCode.UNAVAILABLE,
    grpc.StatusCode.DEADLINE_EXCEEDED,
    grpc.StatusCode.RESOURCE_EXHAUSTED,
    grpc.StatusCode.ABORTED,
}
_TRUE_VALUES = {"1", "true", "yes", "on"}


def _retryable(exc: BaseException) -> bool:
    if isinstance(exc, RuntimeFailure):
        return (
            exc.retryable
            or exc.code is ErrorCode.INFRASTRUCTURE_UNAVAILABLE
        )
    if isinstance(exc, grpc.aio.AioRpcError):
        return exc.code() in _RETRYABLE_GRPC_CODES
    return isinstance(exc, (ConnectionError, OSError, TimeoutError))


class Dispatcher:
    def __init__(self) -> None:
        self.config = LauncherConfig.from_environment()
        self.engine = create_engine(
            self.config.require_database_url(),
            pool_size=int(
                os.environ.get("UR_DISPATCHER_DB_POOL_SIZE", "5")
            ),
            max_overflow=int(
                os.environ.get("UR_DISPATCHER_DB_MAX_OVERFLOW", "5")
            ),
        )
        sessions = create_session_factory(self.engine)
        self.runs = PostgresRunRepository(sessions)
        self.threads = PostgresThreadRepository(sessions)
        self.events = PostgresEventJournal(sessions)
        self.workers = PostgresWorkerRegistry(sessions)
        topics = TopicNames.from_config(
            prefix=self.config.topic_prefix,
            environment=self.config.kafka_environment,
        )
        self.queue = AioKafkaRunCommandQueue(
            bootstrap_servers=self.config.kafka_bootstrap_servers,
            topics=topics,
            group_id=os.environ.get(
                "UR_DISPATCHER_GROUP_ID",
                "runtime.dispatcher",
            ),
        )
        self.execution_client = GrpcExecutionClient()
        self._fallback_targets = tuple(
            target.strip()
            for target in os.environ.get("UR_WORKER_TARGETS", "").split(",")
            if target.strip()
        )
        self._allow_static_fallback = (
            os.environ.get(
                "UR_ALLOW_STATIC_WORKER_FALLBACK",
                "false",
            ).lower()
            in _TRUE_VALUES
        )
        self._fallback_index = 0
        self._max_attempts = max(
            1,
            int(os.environ.get("UR_DISPATCH_MAX_ATTEMPTS", "5")),
        )
        self._retry_base_seconds = max(
            0.05,
            float(
                os.environ.get(
                    "UR_DISPATCH_RETRY_BASE_SECONDS",
                    "0.5",
                )
            ),
        )
        self._retry_max_seconds = max(
            self._retry_base_seconds,
            float(
                os.environ.get(
                    "UR_DISPATCH_RETRY_MAX_SECONDS",
                    "10",
                )
            ),
        )
        self._lease_grace_seconds = max(
            30,
            int(
                os.environ.get(
                    "UR_WORKER_LEASE_GRACE_SECONDS",
                    "60",
                )
            ),
        )

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                receipt = await self.queue.receive(self._dispatcher_id())
                await self._dispatch(receipt)
            except asyncio.CancelledError:
                raise
            except Exception:
                _LOGGER.exception("dispatcher loop iteration failed")
                await asyncio.sleep(self._retry_base_seconds)

    @staticmethod
    def _dispatcher_id() -> WorkerId:
        return WorkerId.parse(
            os.environ.get("UR_INSTANCE_ID", "dispatcher")
        )

    async def _acquire_worker(
        self,
        receipt: RunCommandReceipt,
    ) -> tuple[str, WorkerLease | None]:
        request = receipt.command.request
        now = datetime.now(UTC)
        expires_at = now + timedelta(
            seconds=request.timeout_seconds + self._lease_grace_seconds
        )
        try:
            lease = await self.workers.acquire(
                receipt.identity.deployment_id,
                request.target.graph_id,
                receipt.identity.run_id,
                now=now,
                expires_at=expires_at,
            )
            return lease.grpc_target, lease
        except RuntimeFailure:
            if (
                not self._allow_static_fallback
                or not self._fallback_targets
            ):
                raise
            target = self._fallback_targets[
                self._fallback_index % len(self._fallback_targets)
            ]
            self._fallback_index += 1
            _LOGGER.warning(
                "using explicitly enabled static worker fallback "
                "run_id=%s target=%s",
                receipt.identity.run_id,
                target,
            )
            return target, None

    async def _release_worker(
        self,
        lease: WorkerLease | None,
    ) -> None:
        if lease is None:
            return
        try:
            await self.workers.release(
                lease.lease_id,
                now=datetime.now(UTC),
            )
        except Exception:
            _LOGGER.exception(
                "worker lease release failed lease_id=%s run_id=%s",
                lease.lease_id,
                lease.run_id,
            )

    def _retry_delay(self, delivery_count: int) -> float:
        return float(
            min(
                self._retry_base_seconds
                * (2 ** max(0, delivery_count - 1)),
                self._retry_max_seconds,
            )
        )

    async def _requeue(
        self,
        receipt: RunCommandReceipt,
        run: Run,
        exc: BaseException,
    ) -> None:
        current = await self.runs.get(str(run.run_id))
        if current.status is RunStatus.RUNNING:
            await self.runs.update(current.requeue(datetime.now(UTC)))
        await self.events.append(
            RuntimeEventDraft(
                receipt.identity,
                RuntimeEventType.RUN_QUEUED,
                data={
                    "retry": True,
                    "delivery_count": receipt.delivery_count,
                    "reason": str(exc),
                    "graph_id": receipt.command.request.target.graph_id,
                },
                native={"runtime.retryable": True},
            )
        )
        await self.queue.reject(receipt, retryable=True)
        await asyncio.sleep(
            self._retry_delay(receipt.delivery_count)
        )

    async def _fail_terminal(
        self,
        receipt: RunCommandReceipt,
        run: Run,
        exc: BaseException,
        *,
        worker_target: str | None,
    ) -> None:
        current = await self.runs.get(str(run.run_id))
        if current.status not in _TERMINAL_RUN_STATUSES:
            await self.events.append(
                RuntimeEventDraft(
                    receipt.identity,
                    RuntimeEventType.RUN_FAILED,
                    data={
                        "error": str(exc),
                        "worker_target": worker_target,
                        "delivery_count": receipt.delivery_count,
                    },
                )
            )
            await self.runs.update(
                current.fail(
                    RunError("DISPATCH_FAILED", str(exc)),
                    datetime.now(UTC),
                )
            )
            if current.thread_id is not None:
                thread = await self.threads.get(
                    str(current.thread_id)
                )
                await self.threads.update(
                    thread.mark_error(datetime.now(UTC))
                )
        await self.queue.reject(receipt, retryable=False)

    async def _apply_terminal_event(
        self,
        run: Run,
        draft: RuntimeEventDraft,
        last_result: JsonValue,
    ) -> None:
        current = await self.runs.get(str(run.run_id))
        now = datetime.now(UTC)
        if draft.type is RuntimeEventType.RUN_COMPLETED:
            await self.runs.update(current.complete(last_result, now))
        elif draft.type is RuntimeEventType.RUN_INTERRUPTED:
            await self.runs.update(current.mark_interrupted(now))
        elif draft.type is RuntimeEventType.RUN_CANCELLED:
            await self.runs.update(current.cancel(now))
        elif draft.type is RuntimeEventType.RUN_TIMEOUT:
            await self.runs.update(
                current.fail(
                    RunError("FRAMEWORK_EXECUTION_TIMEOUT", str(draft.data)),
                    now,
                )
            )
        else:
            await self.runs.update(
                current.fail(
                    RunError(
                        "FRAMEWORK_EXECUTION_FAILED",
                        str(draft.data),
                    ),
                    now,
                )
            )

    async def _dispatch(self, receipt: RunCommandReceipt) -> None:
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

        worker_target: str | None = None
        worker_lease: WorkerLease | None = None
        try:
            worker_target, worker_lease = await self._acquire_worker(
                receipt
            )
            await self.runs.update(
                run.mark_running(datetime.now(UTC))
            )
            terminal = False
            last_result: JsonValue = None
            async for draft in self.execution_client.stream(
                worker_target,
                receipt.command,
            ):
                await self.events.append(draft)
                if draft.type is RuntimeEventType.STATE_VALUES:
                    last_result = draft.data
                if draft.type in _TERMINAL_EVENT_TYPES:
                    terminal = True
                    await self._apply_terminal_event(
                        run,
                        draft,
                        last_result,
                    )
            if not terminal:
                current = await self.runs.get(str(run.run_id))
                await self.runs.update(
                    current.complete(last_result, datetime.now(UTC))
                )
            if run.thread_id is not None:
                thread = await self.threads.get(str(run.thread_id))
                await self.threads.update(
                    thread.mark_idle(datetime.now(UTC))
                )
            await self.queue.acknowledge(receipt)
        except asyncio.CancelledError as exc:
            await self._release_worker(worker_lease)
            worker_lease = None
            if receipt.delivery_count < self._max_attempts:
                await self._requeue(receipt, run, exc)
            else:
                await self._fail_terminal(
                    receipt,
                    run,
                    exc,
                    worker_target=worker_target,
                )
            raise
        except Exception as exc:
            current = await self.runs.get(str(run.run_id))
            if current.status in _TERMINAL_RUN_STATUSES:
                await self.queue.acknowledge(receipt)
                return
            await self._release_worker(worker_lease)
            worker_lease = None
            if (
                _retryable(exc)
                and receipt.delivery_count < self._max_attempts
            ):
                _LOGGER.warning(
                    "retrying run dispatch run_id=%s delivery=%s "
                    "error=%s",
                    run.run_id,
                    receipt.delivery_count,
                    exc,
                )
                await self._requeue(receipt, run, exc)
                return
            await self._fail_terminal(
                receipt,
                run,
                exc,
                worker_target=worker_target,
            )
        finally:
            await self._release_worker(worker_lease)

    async def close(self) -> None:
        await self.queue.close()
        await self.execution_client.close()
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
            prefix=config.topic_prefix,
            environment=config.kafka_environment,
        ),
        group_id=os.environ.get(
            "UR_DISPATCHER_GROUP_ID",
            "runtime.dispatcher",
        ),
    )


def main(*, run_forever: bool = False) -> int:
    config = LauncherConfig.from_environment()
    config.validate()
    if run_forever:
        asyncio.run(_serve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main(run_forever=True))
