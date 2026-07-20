from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.domain.events import RuntimeEvent, RuntimeEventDraft, RuntimeEventType
from universal_runtime.domain.execution import (
    ExecutionRequest,
    ExecutionTarget,
    Run,
    RunError,
    RunStatus,
    Thread,
)
from universal_runtime.domain.execution.requests import RunCommand
from universal_runtime.domain.identity import (
    ApplicationScope,
    CommandId,
    ExecutionIdentity,
    RunId,
    ThreadId,
    WorkerId,
)
from universal_runtime.domain.primitives.json_types import JsonObject, JsonValue
from universal_runtime.ports.control_plane import ExecutionPlanResolver
from universal_runtime.ports.events import EventJournal, EventReplay, EventSubscription
from universal_runtime.ports.outbox import OutboxMessage, OutboxRepository
from universal_runtime.ports.queue import RunCommandQueue
from universal_runtime.ports.registry import AdapterRegistry
from universal_runtime.ports.repositories import (
    AssistantRepository,
    RunRepository,
    ThreadRepository,
)
from universal_runtime.ports.runtime_adapter import RuntimeAdapter


def _now() -> datetime:
    return datetime.now(UTC)


_LOGGER = logging.getLogger(__name__)


class RuntimeExecutionService:
    def __init__(
        self,
        *,
        threads: ThreadRepository,
        runs: RunRepository,
        commands: RunCommandQueue,
        journal: EventJournal,
        replay: EventReplay | None = None,
        subscription: EventSubscription | None = None,
        outbox: OutboxRepository | None = None,
        assistants: AssistantRepository | None = None,
        plan_resolver: ExecutionPlanResolver | None = None,
        execution_scope: ApplicationScope | None = None,
        adapters: AdapterRegistry | None = None,
        capacity: Any | None = None,
    ) -> None:
        self._threads = threads
        self._runs = runs
        self._commands = commands
        self._journal = journal
        self._replay = replay
        self._subscription = subscription
        self._outbox = outbox
        self._assistants = assistants
        self._plan_resolver = plan_resolver
        self._execution_scope = execution_scope
        self._adapters = adapters
        self._capacity = capacity
        self._worker_task: asyncio.Task[None] | None = None
        self._active_adapters: dict[str, RuntimeAdapter] = {}

    async def create_thread(
        self, thread_id: str | None = None, metadata: JsonObject | None = None
    ) -> Thread:
        now = _now()
        thread = Thread(
            thread_id=ThreadId.parse(thread_id) if thread_id else ThreadId.new(),
            metadata=metadata or {},
            created_at=now,
            updated_at=now,
        )
        return await self._threads.create(thread)

    @staticmethod
    def _with_scope(request: ExecutionRequest, scope: ApplicationScope) -> ExecutionRequest:
        identity = request.identity
        if identity.scope == scope:
            return request
        return replace(
            request,
            identity=ExecutionIdentity(
                scope,
                identity.assistant_id,
                identity.run_id,
                identity.attempt_id,
                identity.thread_id,
            ),
        )

    async def _resolve_request(
        self,
        request: ExecutionRequest,
        *,
        pinned_scope: ApplicationScope | None = None,
        pinned_target: ExecutionTarget | None = None,
    ) -> ExecutionRequest:
        """Resolve authoritative scope, assistant config and executable graph.

        New runs resolve the active assistant version and active deployment. Resume
        requests pass the scope and target pinned on the original run, so a rolling
        deployment or assistant update cannot move an interrupted execution.
        """
        if self._plan_resolver is not None:
            requested_version = (
                pinned_target.assistant_version
                if pinned_target is not None
                else (
                    request.target.assistant_version
                    if request.target.graph_id != "default"
                    else None
                )
            )
            plan = await self._plan_resolver.resolve(
                request.identity.assistant_id,
                version=requested_version,
            )
            if (
                request.target.graph_id != "default"
                and request.target.graph_id != plan.target.graph_id
            ):
                raise RuntimeFailure(
                    ErrorCode.INVALID_EXECUTION_INPUT,
                    "execution target graph does not match assistant graph",
                    details={
                        "assistant_id": str(plan.assistant.assistant_id),
                        "assistant_graph_id": plan.assistant.graph_id,
                        "requested_graph_id": request.target.graph_id,
                    },
                )
            target = pinned_target or plan.target
            if target.graph_id != plan.assistant.graph_id:
                raise RuntimeFailure(
                    ErrorCode.INVALID_EXECUTION_INPUT,
                    "pinned execution target no longer belongs to the assistant",
                )
            request = self._with_scope(request, pinned_scope or plan.scope)
            return replace(
                request,
                target=target,
                config={**plan.assistant.config, **request.config},
                context={**plan.assistant.context, **request.context},
                metadata={**plan.assistant.metadata, **request.metadata},
            )

        if pinned_scope is not None:
            request = self._with_scope(request, pinned_scope)
        elif self._execution_scope is not None:
            request = self._with_scope(request, self._execution_scope)
        if self._assistants is None:
            return replace(request, target=pinned_target or request.target)
        assistant = await self._assistants.get(str(request.identity.assistant_id))
        target = pinned_target or request.target
        if target.graph_id == "default":
            target = ExecutionTarget(assistant.graph_id, assistant.version)
        elif target.graph_id != assistant.graph_id:
            raise RuntimeFailure(
                ErrorCode.INVALID_EXECUTION_INPUT,
                "execution target graph does not match assistant graph",
                details={
                    "assistant_id": str(assistant.assistant_id),
                    "assistant_graph_id": assistant.graph_id,
                    "requested_graph_id": target.graph_id,
                },
            )
        return replace(
            request,
            target=target,
            config={**assistant.config, **request.config},
            context={**assistant.context, **request.context},
            metadata={**assistant.metadata, **request.metadata},
        )

    async def start_run(
        self, request: ExecutionRequest, *, outbox: OutboxRepository | None = None
    ) -> Run:
        request = await self._resolve_request(request)
        now = _now()
        thread_id = request.identity.thread_id
        if thread_id is not None:
            thread = await self._threads.get(str(thread_id))
            if thread.status.value == "busy":
                raise RuntimeFailure(ErrorCode.THREAD_BUSY, f"thread is busy: {thread_id}")
            await self._threads.update(thread.mark_busy(now))
        run = Run(
            identity=request.identity,
            metadata=request.metadata,
            created_at=now,
            updated_at=now,
            target=request.target,
        )
        created = await self._runs.create(run)
        try:
            selected_outbox = outbox or self._outbox
            await self._journal.append(
                RuntimeEventDraft(
                    request.identity,
                    RuntimeEventType.RUN_QUEUED,
                    data={
                        "assistant_id": str(request.identity.assistant_id),
                        "assistant_version": request.target.assistant_version,
                        "graph_id": request.target.graph_id,
                    },
                )
            )
            if selected_outbox is not None:
                await selected_outbox.append(
                    OutboxMessage(
                        message_id=CommandId.new(),
                        topic="rt.local.run.commands.v1",
                        key=f"{request.identity.application_id}:{request.identity.thread_id or 'stateless'}",
                        payload={
                            "run_id": str(request.identity.run_id),
                            "assistant_id": str(request.identity.assistant_id),
                            "assistant_version": request.target.assistant_version,
                            "graph_id": request.target.graph_id,
                            "thread_id": (
                                str(request.identity.thread_id)
                                if request.identity.thread_id is not None
                                else None
                            ),
                            "input": request.input,
                            "command": request.command,
                            "config": request.config,
                            "context": request.context,
                            "metadata": request.metadata,
                            "stream_modes": list(request.stream_modes),
                        },
                        created_at=now,
                    )
                )
            else:
                await self._commands.publish(
                    RunCommand(
                        command_id=CommandId.new(),
                        identity=request.identity,
                        request=request,
                        priority=request.priority,
                        available_at=now,
                        created_at=now,
                    )
                )
        except Exception:
            await self._runs.update(
                created.fail(
                    RunError("RUN_QUEUE_FAILED", "run command could not be queued"), _now()
                )
            )
            if thread_id is not None:
                thread = await self._threads.get(str(thread_id))
                await self._threads.update(thread.mark_error(_now()))
            raise
        return created

    async def cancel_run(self, run_id: str) -> Run:
        run = await self._runs.get(run_id)
        if run.status in {
            RunStatus.SUCCESS,
            RunStatus.ERROR,
            RunStatus.TIMEOUT,
            RunStatus.CANCELLED,
        }:
            return run
        adapter = self._active_adapters.get(str(run.run_id))
        if adapter is not None:
            request = ExecutionRequest(identity=run.identity, target=run.target)
            await adapter.cancel(request)
        cancelled = run.cancel(_now())
        if cancelled is not run:
            await self._runs.update(cancelled)
        if run.thread_id is not None:
            thread = await self._threads.get(str(run.thread_id))
            await self._threads.update(thread.mark_idle(_now()))
        await self._journal.append(
            RuntimeEventDraft(
                run.identity,
                RuntimeEventType.RUN_CANCELLED,
                data={"run_id": str(run.run_id), "status": str(cancelled.status)},
            )
        )
        return cancelled

    async def resume_run(self, request: ExecutionRequest) -> Run:
        run = await self._runs.get(str(request.identity.run_id))
        if run.status is not RunStatus.INTERRUPTED:
            raise RuntimeFailure(ErrorCode.INVALID_EXECUTION_INPUT, "run is not interrupted")
        request = await self._resolve_request(
            replace(request, identity=run.identity, target=run.target),
            pinned_scope=run.identity.scope,
            pinned_target=run.target,
        )
        now = _now()
        await self._commands.publish(
            RunCommand(
                command_id=CommandId.new(),
                identity=run.identity,
                request=replace(request, identity=run.identity),
                priority=request.priority,
                available_at=now,
                created_at=now,
            )
        )
        await self._journal.append(
            RuntimeEventDraft(run.identity, RuntimeEventType.RUN_QUEUED, data={"resume": True})
        )
        return run

    async def start_worker(self) -> None:
        if self._worker_task is not None:
            return
        if self._adapters is None or not self._adapters.all():
            return
        self._worker_task = asyncio.create_task(self._consume_commands())

    async def stop_worker(self) -> None:
        task = self._worker_task
        self._worker_task = None
        if task is None:
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def _consume_commands(self) -> None:
        assert self._adapters is not None
        while True:
            try:
                receipt = await self._commands.receive(WorkerId.parse("local-worker"))
            except (RuntimeFailure, asyncio.CancelledError):
                return
            request = receipt.command.request
            adapter = self._adapters.get(request.target.graph_id)
            _LOGGER.info(
                "runtime command received run_id=%s graph_id=%s",
                receipt.identity.run_id,
                request.target.graph_id,
            )
            execution: asyncio.Task[None] | None = None
            try:
                if self._capacity is None:
                    execution = asyncio.create_task(self._execute_receipt(receipt, adapter))
                    await execution
                else:
                    async with self._capacity.slot():
                        execution = asyncio.create_task(self._execute_receipt(receipt, adapter))
                        await execution
            except asyncio.CancelledError:
                if execution is not None and execution.cancelled():
                    continue
                raise
            except Exception:
                _LOGGER.exception(
                    "runtime command execution failed run_id=%s", receipt.identity.run_id
                )
                raise

    async def _execute_receipt(self, receipt: Any, adapter: RuntimeAdapter) -> None:
        request = receipt.command.request
        run = await self._runs.get(str(request.identity.run_id))
        if run.status is not RunStatus.PENDING:
            _LOGGER.info("duplicate command ignored run_id=%s status=%s", run.run_id, run.status)
            await self._commands.acknowledge(receipt)
            return
        self._active_adapters[str(run.run_id)] = adapter
        last_result: JsonObject | JsonValue = None
        terminal = False
        try:
            _LOGGER.info("runtime execution started run_id=%s", run.run_id)
            await self._runs.update(run.mark_running(_now()))
            async for draft in adapter.stream(request):
                _LOGGER.info("runtime event received run_id=%s type=%s", run.run_id, draft.type)
                event = await self._journal.append(draft)
                if event.type is RuntimeEventType.STATE_VALUES:
                    last_result = event.data
                if event.type in {
                    RuntimeEventType.RUN_COMPLETED,
                    RuntimeEventType.RUN_CANCELLED,
                    RuntimeEventType.RUN_FAILED,
                    RuntimeEventType.RUN_TIMEOUT,
                    RuntimeEventType.RUN_INTERRUPTED,
                }:
                    terminal = True
                    current = await self._runs.get(str(run.run_id))
                    if event.type is RuntimeEventType.RUN_COMPLETED:
                        await self._runs.update(current.complete(last_result, _now()))
                    elif event.type is RuntimeEventType.RUN_CANCELLED:
                        await self._runs.update(current.cancel(_now()))
                    elif event.type is RuntimeEventType.RUN_INTERRUPTED:
                        await self._runs.update(current.mark_interrupted(_now()))
                    else:
                        await self._runs.update(
                            current.fail(
                                RunError("FRAMEWORK_EXECUTION_FAILED", str(event.data)), _now()
                            )
                        )
                    if current.thread_id is not None:
                        thread = await self._threads.get(str(current.thread_id))
                        await self._threads.update(
                            thread.mark_interrupted(_now())
                            if event.type is RuntimeEventType.RUN_INTERRUPTED
                            else thread.mark_idle(_now())
                        )
            if not terminal:
                current = await self._runs.get(str(run.run_id))
                await self._runs.update(current.complete(last_result, _now()))
        except asyncio.CancelledError:
            current = await self._runs.get(str(run.run_id))
            if current.status is not RunStatus.CANCELLED:
                await self._journal.append(
                    RuntimeEventDraft(request.identity, RuntimeEventType.RUN_CANCELLED)
                )
                await self._runs.update(current.cancel(_now()))
            return
        except Exception as exc:
            _LOGGER.exception("runtime execution crashed run_id=%s", run.run_id)
            current = await self._runs.get(str(run.run_id))
            if current.status not in {
                RunStatus.SUCCESS,
                RunStatus.ERROR,
                RunStatus.TIMEOUT,
                RunStatus.CANCELLED,
            }:
                await self._journal.append(
                    RuntimeEventDraft(
                        request.identity,
                        RuntimeEventType.RUN_FAILED,
                        data={"error": str(exc)},
                    )
                )
                await self._runs.update(
                    current.fail(RunError("FRAMEWORK_EXECUTION_FAILED", str(exc)), _now())
                )
            raise
        finally:
            self._active_adapters.pop(str(run.run_id), None)
            if run.thread_id is not None:
                thread = await self._threads.get(str(run.thread_id))
                if thread.status.value == "busy":
                    await self._threads.update(thread.mark_idle(_now()))
            await self._commands.acknowledge(receipt)

    async def stream_events(
        self, run_id: str, after_sequence: int = -1
    ) -> AsyncIterator[RuntimeEvent]:
        if self._replay is None:
            raise RuntimeFailure(ErrorCode.ADAPTER_NOT_SUPPORTED, "event replay is not configured")
        for event in await self._replay.replay(RunId.parse(run_id), after_sequence=after_sequence):
            yield event

    async def stream_live_events(
        self, run_id: str, after_sequence: int = -1
    ) -> AsyncIterator[RuntimeEvent]:
        if self._subscription is None:
            raise RuntimeFailure(
                ErrorCode.ADAPTER_NOT_SUPPORTED, "event subscription is not configured"
            )
        async for event in self._subscription.subscribe(
            RunId.parse(run_id), after_sequence=after_sequence
        ):
            yield event
