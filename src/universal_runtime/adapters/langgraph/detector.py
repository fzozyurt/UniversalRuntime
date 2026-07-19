from __future__ import annotations

from typing import Any

from universal_runtime.adapters.langgraph.descriptor import (
    GraphObjectKind,
    LangGraphDescriptor,
    LangGraphProfile,
)


def detect_graph(
    target: Any,
    *,
    entrypoint: str = "<object>",
    profile: LangGraphProfile | None = None,
) -> LangGraphDescriptor:
    module = type(target).__module__.lower()
    is_compiled = hasattr(target, "astream") and hasattr(target, "ainvoke")
    is_factory = callable(target) and not is_compiled
    kind = (
        GraphObjectKind.COMPILED
        if is_compiled
        else GraphObjectKind.FACTORY
        if is_factory
        else GraphObjectKind.BUILDER
    )
    config = getattr(target, "config", {})
    metadata = config.get("metadata", {}) if isinstance(config, dict) else {}
    integration = str(metadata.get("ls_integration", ""))
    if profile is not None:
        resolved_profile = profile
    elif integration == "deepagents" or module.startswith("deepagents"):
        resolved_profile = LangGraphProfile.DEEPAGENTS
    elif integration == "langchain_create_agent" or module.startswith("langchain"):
        resolved_profile = LangGraphProfile.LANGCHAIN_AGENT
    else:
        resolved_profile = LangGraphProfile.LANGGRAPH
    if is_compiled and resolved_profile in {
        LangGraphProfile.LANGCHAIN_AGENT,
        LangGraphProfile.DEEPAGENTS,
    }:
        kind = GraphObjectKind.AGENT
    return LangGraphDescriptor(
        profile=resolved_profile,
        entrypoint=entrypoint,
        object_kind=kind,
        graph_id=str(
            getattr(target, "name", None) or getattr(target, "graph_id", None) or entrypoint
        ),
        has_checkpointer=getattr(target, "checkpointer", None) is not None,
        has_store=getattr(target, "store", None) is not None,
        input_schema=_schema(target, "input_schema"),
        output_schema=_schema(target, "output_schema"),
        state_schema=_schema(target, "state_schema"),
        config_schema=_schema(target, "config_schema"),
        context_schema=_schema(target, "context_schema"),
    )


def _schema(target: Any, name: str) -> dict[str, Any] | None:
    try:
        value = getattr(target, name, None)
    except Exception:
        return None
    if value is None:
        return None
    if isinstance(value, dict):
        return dict(value)
    fields = getattr(value, "__annotations__", None)
    return {"fields": list(fields)} if fields else {"type": repr(value)}


def is_compiled_graph(target: Any) -> bool:
    return hasattr(target, "astream") and hasattr(target, "ainvoke")
