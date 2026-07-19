from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import replace
from typing import Any

from universal_runtime.adapters.langgraph.config_mapper import map_config
from universal_runtime.adapters.langgraph.descriptor import LangGraphDescriptor
from universal_runtime.adapters.langgraph.detector import detect_graph
from universal_runtime.adapters.langgraph.errors import LangGraphAdapterError, LangGraphErrorCode
from universal_runtime.adapters.langgraph.interrupt_adapter import resume_command
from universal_runtime.adapters.langgraph.loader import load_graph
from universal_runtime.adapters.langgraph.manifest import langgraph_manifest
from universal_runtime.adapters.langgraph.persistence import local_persistence, validate_persistence
from universal_runtime.adapters.langgraph.state_adapter import get_state, get_state_history
from universal_runtime.adapters.langgraph.stream_mapper import map_stream
from universal_runtime.domain.capabilities import AdapterCapabilities, AdapterManifest
from universal_runtime.domain.events import RuntimeEventDraft, RuntimeEventType
from universal_runtime.domain.execution import ExecutionRequest


class LangGraphAdapter:
    def __init__(self, target: Any, *, persistence_mode: str = "disabled") -> None:
        source_descriptor = detect_graph(target)
        validate_persistence(persistence_mode, has_checkpointer=source_descriptor.has_checkpointer)
        providers = (
            local_persistence(persistence_mode)
            if persistence_mode == "platform-managed"
            else local_persistence("disabled")
        )
        self._graph = load_graph(target, checkpointer=providers.checkpointer, store=providers.store)
        self._descriptor: LangGraphDescriptor = detect_graph(self._graph)
        self._manifest = self._manifest_for_graph(langgraph_manifest())
        self._tasks: dict[str, asyncio.Task[Any]] = {}

    def _manifest_for_graph(self, manifest: AdapterManifest) -> AdapterManifest:
        capabilities = manifest.capabilities
        has_state = self._descriptor.has_checkpointer
        return replace(
            manifest,
            capabilities=AdapterCapabilities(
                streaming=capabilities.streaming,
                cancellation=capabilities.cancellation,
                history=has_state,
                checkpoint=has_state,
                state_management=has_state,
                interrupt=has_state,
                resume=has_state,
                fork=capabilities.fork,
                custom_http=capabilities.custom_http,
                a2a=capabilities.a2a,
                subagents=capabilities.subagents,
            ),
        )

    @property
    def manifest(self) -> AdapterManifest:
        return self._manifest

    @property
    def descriptor(self) -> LangGraphDescriptor:
        return self._descriptor

    async def inspect(self, target: Any | None = None) -> LangGraphDescriptor:
        return detect_graph(self._graph if target is None else target)

    async def invoke(self, request: ExecutionRequest) -> Any:
        task = asyncio.current_task()
        key = str(request.identity.run_id)
        if task is not None:
            self._tasks[key] = task
        config = map_config(request)
        kwargs: dict[str, Any] = {"config": config}
        if request.context:
            kwargs["context"] = request.context
        payload = request.command if request.command is not None else request.input
        try:
            try:
                return await self._graph.ainvoke(payload, **kwargs)
            except TypeError as exc:
                if "context" not in str(exc):
                    raise
                kwargs.pop("context", None)
                return await self._graph.ainvoke(payload, **kwargs)
        finally:
            self._tasks.pop(key, None)

    async def stream(self, request: ExecutionRequest) -> AsyncIterator[RuntimeEventDraft]:
        task = asyncio.current_task()
        key = str(request.identity.run_id)
        if task is not None:
            self._tasks[key] = task
        config = map_config(request)
        kwargs: dict[str, Any] = {
            "config": config,
            "stream_mode": list(request.stream_modes)
            if len(request.stream_modes) > 1
            else request.stream_modes[0],
        }
        if request.stream_subgraphs:
            kwargs["subgraphs"] = True
        if request.context:
            kwargs["context"] = request.context
        payload = request.command if request.command is not None else request.input
        yield RuntimeEventDraft(
            request.identity, RuntimeEventType.RUN_STARTED, data={"run_id": key}
        )
        try:
            try:
                chunks = self._graph.astream(payload, **kwargs)
                async for event in map_stream(chunks, request):
                    yield event
            except TypeError as exc:
                if "context" not in str(exc):
                    raise
                kwargs.pop("context", None)
                async for event in map_stream(self._graph.astream(payload, **kwargs), request):
                    yield event
        except asyncio.CancelledError:
            yield RuntimeEventDraft(
                request.identity, RuntimeEventType.RUN_CANCELLED, data={"run_id": key}
            )
            raise
        except Exception as exc:
            yield RuntimeEventDraft(
                request.identity,
                RuntimeEventType.RUN_FAILED,
                data={"run_id": key, "error": str(exc)},
            )
            raise
        else:
            yield RuntimeEventDraft(
                request.identity, RuntimeEventType.RUN_COMPLETED, data={"run_id": key}
            )
        finally:
            self._tasks.pop(key, None)

    async def cancel(self, request: ExecutionRequest) -> None:
        task = self._tasks.get(str(request.identity.run_id))
        if task is None:
            return
        if task is asyncio.current_task():
            raise LangGraphAdapterError(
                LangGraphErrorCode.CAPABILITY_NOT_SUPPORTED,
                "a run cannot cancel itself",
            )
        task.cancel()

    async def get_state(self, request: ExecutionRequest) -> Any:
        return await get_state(self._graph, map_config(request))

    async def get_state_history(self, request: ExecutionRequest) -> Any:
        return await get_state_history(self._graph, map_config(request))

    async def update_state(self, request: ExecutionRequest, values: Any) -> Any:
        if not hasattr(self._graph, "aupdate_state"):
            raise LangGraphAdapterError(
                LangGraphErrorCode.CAPABILITY_NOT_SUPPORTED, "state update is not supported"
            )
        return await self._graph.aupdate_state(map_config(request), values)

    async def resume(self, request: ExecutionRequest, value: Any) -> Any:
        if request.identity.thread_id is None:
            raise LangGraphAdapterError(
                LangGraphErrorCode.INVALID_GRAPH, "resume requires a thread"
            )
        kwargs: dict[str, Any] = {"config": map_config(request)}
        if request.context:
            kwargs["context"] = request.context
        try:
            return await self._graph.ainvoke(resume_command(value), **kwargs)
        except TypeError as exc:
            if "context" not in str(exc):
                raise
            kwargs.pop("context", None)
            return await self._graph.ainvoke(resume_command(value), **kwargs)
