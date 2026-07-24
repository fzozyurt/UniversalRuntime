from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

from universal_runtime.adapters.kafka import AioKafkaRunCommandQueue
from universal_runtime.adapters.langgraph import LangGraphAdapter
from universal_runtime.domain.events import RuntimeEventType
from universal_runtime.domain.execution import RunError, RunStatus

_LOGGER = logging.getLogger(__name__)

TERMINAL_EVENT_TYPES = {
    RuntimeEventType.RUN_COMPLETED,
    RuntimeEventType.RUN_FAILED,
    RuntimeEventType.RUN_CANCELLED,
    RuntimeEventType.RUN_INTERRUPTED,
}

FINAL_STATUSES = {
    RunStatus.SUCCESS,
    RunStatus.ERROR,
    RunStatus.TIMEOUT,
    RunStatus.CANCELLED,
    RunStatus.INTERRUPTED,
}


async def process_receipt(
    receipt: Any,
    bounded_worker: Any,
    event_publisher: Any,
    queue: AioKafkaRunCommandQueue,
    adapters: dict[str, LangGraphAdapter],
    runs: Any,
    threads: Any,
) -> None:
    try:
        run = await runs.get(str(receipt.identity.run_id))
        if run.status is not RunStatus.PENDING:
            await queue.acknowledge(receipt)
            return

        adapter = adapters.get(str(receipt.identity.assistant_id))
        if adapter is None:
            await runs.update(
                run.fail(
                    RunError(
                        "ADAPTER_NOT_FOUND",
                        f"no adapter registered for assistant {receipt.identity.assistant_id}",
                    ),
                    datetime.now(UTC),
                )
            )
            await queue.acknowledge(receipt)
            return

        await runs.update(run.mark_running(datetime.now(UTC)))
        await _stream_execution(
            receipt,
            run,
            adapter,
            event_publisher,
            runs,
            threads,
        )
        await queue.acknowledge(receipt)
    except asyncio.CancelledError:
        await queue.reject(receipt, retryable=True)
        raise
    except Exception:
        _LOGGER.exception("worker execution failed run_id=%s", receipt.identity.run_id)
        await queue.reject(receipt, retryable=True)
    finally:
        bounded_worker.release()


async def _stream_execution(
    receipt: Any,
    run: Any,
    adapter: Any,
    event_publisher: Any,
    runs: Any,
    threads: Any,
) -> None:
    sequence = 0
    try:
        async for draft in adapter.stream(receipt.command.request):
            if event_publisher is not None:
                await event_publisher.publish(_event_to_proto(run.identity, draft, sequence))
            sequence += 1
            await _apply_terminal_event(run, draft, runs, threads)
    except Exception as exc:
        await _fail_if_not_done(run, exc, runs)
        raise
    await _mark_idle_if_needed(run, runs, threads)


def _event_to_proto(identity: Any, draft: Any, sequence: int) -> Any:
    from google.protobuf import struct_pb2

    from universal_runtime.adapters.grpc.generated.runtime.v1 import execution_pb2
    from universal_runtime.adapters.grpc.payloads import python_to_value

    event = execution_pb2.RuntimeEvent(
        type=str(draft.type),
        sequence=sequence,
        namespace=list(draft.namespace),
        data=python_to_value(draft.data),
        native=struct_pb2.Struct(
            fields={key: python_to_value(value) for key, value in draft.native.items()}
        ),
    )
    event.identity.CopyFrom(_identity_to_proto(identity))
    event.timestamp.FromDatetime(datetime.now(UTC))
    return event


async def _apply_terminal_event(run: Any, draft: Any, runs: Any, threads: Any) -> None:
    if draft.type not in TERMINAL_EVENT_TYPES:
        return
    current = await runs.get(str(run.run_id))
    if draft.type is RuntimeEventType.RUN_COMPLETED:
        await runs.update(current.complete(draft.data, datetime.now(UTC)))
    elif draft.type is RuntimeEventType.RUN_CANCELLED:
        await runs.update(current.cancel(datetime.now(UTC)))
    elif draft.type is RuntimeEventType.RUN_INTERRUPTED:
        await runs.update(
            type(current)(
                current.identity,
                RunStatus.INTERRUPTED,
                current.metadata,
                current.created_at,
                datetime.now(UTC),
                current.result,
                current.error,
            )
        )
    else:
        await runs.update(
            current.fail(
                RunError("FRAMEWORK_EXECUTION_FAILED", str(draft.data)),
                datetime.now(UTC),
            )
        )
    if current.thread_id is not None:
        thread = await threads.get(str(current.thread_id))
        await threads.update(
            thread.mark_interrupted(datetime.now(UTC))
            if draft.type is RuntimeEventType.RUN_INTERRUPTED
            else thread.mark_idle(datetime.now(UTC))
        )


async def _fail_if_not_done(run: Any, exc: Exception, runs: Any) -> None:
    current = await runs.get(str(run.run_id))
    if current.status not in FINAL_STATUSES:
        await runs.update(
            current.fail(
                RunError("EXECUTION_FAILED", str(exc)),
                datetime.now(UTC),
            )
        )


async def _mark_idle_if_needed(run: Any, runs: Any, threads: Any) -> None:
    current = await runs.get(str(run.run_id))
    if current.status is RunStatus.INTERRUPTED or current.thread_id is None:
        return
    thread = await threads.get(str(current.thread_id))
    if thread.status.value == "busy":
        await threads.update(thread.mark_idle(datetime.now(UTC)))


def _identity_to_proto(identity: Any) -> Any:
    from universal_runtime.adapters.grpc.generated.runtime.v1 import execution_pb2

    return execution_pb2.ExecutionIdentity(
        workspace_id=str(identity.scope.workspace_id),
        project_id=str(identity.scope.project_id),
        application_id=str(identity.scope.application_id),
        revision_id=str(identity.scope.revision_id),
        deployment_id=str(identity.scope.deployment_id),
        assistant_id=str(identity.assistant_id),
        thread_id=str(identity.thread_id) if identity.thread_id else "",
        run_id=str(identity.run_id),
        attempt_id=str(identity.attempt_id),
    )
