from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from universal_runtime.adapters.a2a.agent_card import build_agent_card
from universal_runtime.adapters.a2a.descriptor import descriptor_from_manifest
from universal_runtime.adapters.a2a.event_mapper import text_message
from universal_runtime.adapters.a2a.request_mapper import execution_request
from universal_runtime.adapters.a2a.status_mapper import task_state
from universal_runtime.domain.assistants import Assistant
from universal_runtime.domain.capabilities import AdapterManifest
from universal_runtime.domain.events import RuntimeEvent
from universal_runtime.domain.identity import (
    ApplicationId,
    ApplicationScope,
    DeploymentId,
    ProjectId,
    RevisionId,
    RunId,
    WorkspaceId,
)


def create_a2a_routes(
    *,
    runtime: Any,
    assistant: Assistant,
    manifest: AdapterManifest,
    public_url: str,
    card_path: str = "/.well-known/agent-card.json",
    task_store: Any | None = None,
) -> Sequence[Any]:
    """Create official SDK routes; user application code is never imported here."""
    try:
        from a2a.server.agent_execution import (
            AgentExecutor,
            RequestContext,
            SimpleRequestContextBuilder,
        )
        from a2a.server.id_generator import IDGenerator
        from a2a.server.request_handlers import DefaultRequestHandler
        from a2a.server.routes import create_agent_card_routes, create_jsonrpc_routes
        from a2a.server.tasks import InMemoryTaskStore
    except ImportError as exc:  # pragma: no cover
        from universal_runtime.domain.errors import ErrorCode, RuntimeFailure

        raise RuntimeFailure(ErrorCode.ADAPTER_NOT_SUPPORTED, "A2A SDK is not installed") from exc

    descriptor = descriptor_from_manifest(
        name=assistant.name or str(assistant.assistant_id),
        description=f"UniversalRuntime assistant {assistant.graph_id}",
        version=str(assistant.version),
        url=public_url,
        manifest=manifest,
    )
    card = build_agent_card(descriptor)

    class RuntimeExecutor(AgentExecutor):
        async def execute(self, context: RequestContext, event_queue: Any) -> None:
            if context.message is None or context.task_id is None:
                return
            run_id = RunId.parse(context.task_id)
            request = execution_request(
                message=context.message,
                assistant_id=assistant.assistant_id,
                run_id=run_id,
                scope=ApplicationScope(
                    WorkspaceId.parse("gateway"),
                    ProjectId.parse("default"),
                    ApplicationId.parse("gateway"),
                    RevisionId.parse("active"),
                    DeploymentId.parse("local"),
                ),
            )
            run = await runtime.execution.start_run(request, outbox=runtime.outbox)
            from a2a.server.tasks import TaskUpdater
            from a2a.types import Task, TaskState, TaskStatus

            await event_queue.enqueue_event(
                Task(
                    id=str(run.run_id),
                    context_id=str(request.identity.thread_id or context.context_id or ""),
                    status=TaskStatus(state=TaskState.TASK_STATE_SUBMITTED),
                )
            )
            updater = TaskUpdater(
                event_queue, str(run.run_id), str(request.identity.thread_id or "")
            )
            async for event in runtime.execution.stream_live_events(str(run.run_id)):
                await _enqueue_event(updater, event)

        async def cancel(self, context: RequestContext, event_queue: Any) -> None:
            if context.task_id is None:
                return
            await runtime.execution.cancel_run(context.task_id)

    class RuntimeIdGenerator(IDGenerator):
        def generate(self, context: Any) -> str:
            del context
            return str(RunId.new())

    configured_task_store = task_store or InMemoryTaskStore()
    handler = DefaultRequestHandler(
        agent_executor=RuntimeExecutor(),
        task_store=configured_task_store,
        agent_card=card,
        request_context_builder=SimpleRequestContextBuilder(
            task_store=configured_task_store,
            task_id_generator=RuntimeIdGenerator(),
        ),
    )
    return [
        *create_agent_card_routes(card, card_url=card_path),
        *create_jsonrpc_routes(handler, rpc_url="/"),
    ]


async def _enqueue_event(updater: Any, event: RuntimeEvent) -> None:
    state = task_state(event.type)
    if state is not None:
        await updater.update_status(state, metadata={"runtime.sequence": event.sequence})
    message = text_message(event)
    if message is not None:
        await updater.event_queue.enqueue_event(message)
