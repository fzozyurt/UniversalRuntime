from __future__ import annotations

import importlib
from typing import Any

from universal_runtime.adapters.langgraph.detector import is_compiled_graph
from universal_runtime.adapters.langgraph.errors import LangGraphAdapterError, LangGraphErrorCode


def load_entrypoint(
    entrypoint: str,
    *,
    checkpointer: Any | None = None,
    store: Any | None = None,
) -> Any:
    try:
        module_name, attribute = entrypoint.split(":", 1)
        target = getattr(importlib.import_module(module_name), attribute)
        return load_graph(target, checkpointer=checkpointer, store=store)
    except LangGraphAdapterError:
        raise
    except Exception as exc:
        raise LangGraphAdapterError(
            LangGraphErrorCode.LOAD_FAILED, f"could not load {entrypoint}"
        ) from exc


def load_graph(target: Any, *, checkpointer: Any | None = None, store: Any | None = None) -> Any:
    if is_compiled_graph(target):
        if checkpointer is not None or store is not None:
            raise LangGraphAdapterError(
                LangGraphErrorCode.INVALID_PERSISTENCE,
                "persistence providers must be injected by a graph factory or builder",
            )
        return target
    if hasattr(target, "compile"):
        return _compile(target, checkpointer=checkpointer, store=store)
    if callable(target):
        return _call_factory(target, checkpointer=checkpointer, store=store)
    raise LangGraphAdapterError(
        LangGraphErrorCode.INVALID_GRAPH, "target is not a LangGraph executable"
    )


def _compile(target: Any, *, checkpointer: Any | None, store: Any | None) -> Any:
    kwargs = {"checkpointer": checkpointer, "store": store}
    kwargs = {key: value for key, value in kwargs.items() if value is not None}
    try:
        return target.compile(**kwargs)
    except TypeError as exc:
        if kwargs and "unexpected keyword" in str(exc):
            raise LangGraphAdapterError(
                LangGraphErrorCode.INVALID_PERSISTENCE,
                "graph builder does not accept managed persistence providers",
            ) from exc
        raise


def _call_factory(target: Any, *, checkpointer: Any | None, store: Any | None) -> Any:
    kwargs = {"checkpointer": checkpointer, "store": store}
    kwargs = {key: value for key, value in kwargs.items() if value is not None}
    fallback = False
    try:
        result = target(**kwargs) if kwargs else target()
    except TypeError as exc:
        if kwargs and "unexpected keyword" in str(exc):
            fallback = True
            result = target()
        else:
            raise
    if hasattr(result, "compile"):
        return _compile(result, checkpointer=checkpointer, store=store)
    if not is_compiled_graph(result):
        raise LangGraphAdapterError(
            LangGraphErrorCode.INVALID_GRAPH,
            "factory did not return a compiled graph or graph builder",
        )
    if fallback:
        raise LangGraphAdapterError(
            LangGraphErrorCode.INVALID_PERSISTENCE,
            "compiled factory did not accept managed persistence providers",
        )
    return result
