from __future__ import annotations

import importlib
from typing import Any

from universal_runtime.adapters.langgraph.detector import is_compiled_graph
from universal_runtime.adapters.langgraph.errors import LangGraphAdapterError, LangGraphErrorCode


def load_entrypoint(entrypoint: str, *, persistence: Any = None) -> Any:
    try:
        module_name, attribute = entrypoint.split(":", 1)
        target = getattr(importlib.import_module(module_name), attribute)
        if is_compiled_graph(target):
            return target
        if callable(target):
            try:
                return target(checkpointer=persistence) if persistence is not None else target()
            except TypeError:
                return target()
        return target
    except Exception as exc:
        raise LangGraphAdapterError(
            LangGraphErrorCode.LOAD_FAILED, f"could not load {entrypoint}"
        ) from exc


def load_graph(target: Any, *, persistence: Any = None) -> Any:
    if is_compiled_graph(target):
        return target
    if hasattr(target, "compile"):
        return (
            target.compile(checkpointer=persistence)
            if persistence is not None
            else target.compile()
        )
    if callable(target):
        result = target(checkpointer=persistence) if persistence is not None else target()
        if not is_compiled_graph(result):
            raise LangGraphAdapterError(
                LangGraphErrorCode.INVALID_GRAPH, "factory did not return a compiled graph"
            )
        return result
    raise LangGraphAdapterError(
        LangGraphErrorCode.INVALID_GRAPH, "target is not a LangGraph executable"
    )
