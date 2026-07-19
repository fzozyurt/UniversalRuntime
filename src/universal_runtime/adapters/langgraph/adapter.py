from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

from universal_runtime.adapters.langgraph.config_mapper import map_config
from universal_runtime.adapters.langgraph.descriptor import LangGraphDescriptor
from universal_runtime.adapters.langgraph.detector import detect_graph
from universal_runtime.adapters.langgraph.errors import LangGraphAdapterError, LangGraphErrorCode
from universal_runtime.adapters.langgraph.interrupt_adapter import resume_command
from universal_runtime.adapters.langgraph.loader import load_graph
from universal_runtime.adapters.langgraph.manifest import langgraph_manifest
from universal_runtime.adapters.langgraph.persistence import local_persistence
from universal_runtime.adapters.langgraph.state_adapter import get_state, get_state_history
from universal_runtime.adapters.langgraph.stream_mapper import map_stream
from universal_runtime.domain.capabilities import AdapterManifest
from universal_runtime.domain.events import RuntimeEventDraft
from universal_runtime.domain.execution import ExecutionRequest


class LangGraphAdapter:
    def __init__(self, target: Any, *, persistence_mode: str = "disabled") -> None:
        providers = local_persistence(persistence_mode)
        self._graph = load_graph(target, persistence=providers.checkpointer)
        self._descriptor: LangGraphDescriptor = detect_graph(self._graph)
        self._manifest = langgraph_manifest()
        self._tasks: dict[str, asyncio.Task[Any]] = {}

    @property
    def manifest(self) -> AdapterManifest:
        return self._manifest

    @property
    def descriptor(self) -> LangGraphDescriptor:
        return self._descriptor

    async def inspect(self, target: Any | None = None) -> LangGraphDescriptor:
        return detect_graph(self._graph if target is None else target)

    async def invoke(self, request: ExecutionRequest) -> Any:
        config = map_config(request)
        kwargs: dict[str, Any] = {"config": config}
        if request.context:
            kwargs["context"] = request.context
        try:
            return await self._graph.ainvoke(request.input, **kwargs)
        except TypeError as exc:
            if "context" not in str(exc):
                raise
            kwargs.pop("context", None)
            return await self._graph.ainvoke(request.input, **kwargs)

    async def stream(self, request: ExecutionRequest) -> AsyncIterator[RuntimeEventDraft]:
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
        try:
            chunks = self._graph.astream(request.input, **kwargs)
            async for event in map_stream(chunks, request):
                yield event
        except TypeError as exc:
            if "context" not in str(exc):
                raise
            kwargs.pop("context", None)
            async for event in map_stream(self._graph.astream(request.input, **kwargs), request):
                yield event

    async def cancel(self, request: ExecutionRequest) -> None:
        task = self._tasks.get(str(request.identity.run_id))
        if task is not None:
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
        return await self._graph.ainvoke(
            resume_command(value), config=map_config(request), context=request.context
        )
