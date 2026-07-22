from __future__ import annotations

from typing import Any

from universal_runtime.adapters.langgraph.errors import LangGraphAdapterError, LangGraphErrorCode


async def get_state(graph: Any, config: dict[str, Any]) -> Any:
    if not hasattr(graph, "aget_state"):
        raise LangGraphAdapterError(
            LangGraphErrorCode.CAPABILITY_NOT_SUPPORTED, "state is not supported"
        )
    try:
        return await graph.aget_state(config)
    except ValueError as exc:
        raise LangGraphAdapterError(
            LangGraphErrorCode.CAPABILITY_NOT_SUPPORTED,
            "state is not available without a checkpointer",
        ) from exc


async def get_state_history(
    graph: Any,
    config: dict[str, Any],
    *,
    before: dict[str, Any] | None = None,
    limit: int | None = None,
    filter: dict[str, Any] | None = None,
) -> Any:
    if not hasattr(graph, "aget_state_history"):
        raise LangGraphAdapterError(
            LangGraphErrorCode.CAPABILITY_NOT_SUPPORTED, "state history is not supported"
        )
    try:
        kwargs: dict[str, Any] = {}
        if before is not None:
            kwargs["before"] = before
        if limit is not None:
            kwargs["limit"] = limit
        if filter is not None:
            kwargs["filter"] = filter
        return [item async for item in graph.aget_state_history(config, **kwargs)]
    except ValueError as exc:
        raise LangGraphAdapterError(
            LangGraphErrorCode.CAPABILITY_NOT_SUPPORTED,
            "state history is not available without a checkpointer",
        ) from exc
