from __future__ import annotations

import logging
from typing import Any

from universal_runtime.adapters.langgraph.descriptor import (
    GraphObjectKind,
    LangGraphDescriptor,
    LangGraphProfile,
)

_LOGGER = logging.getLogger(__name__)


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
    metadata = _export_metadata(target)
    if profile is not None:
        resolved_profile = profile
    elif (exported := _profile_from_metadata(metadata)) is not None:
        resolved_profile = exported
    elif module.startswith("deepagents"):
        resolved_profile = LangGraphProfile.DEEPAGENTS
    elif module.startswith("langchain"):
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
    method_name = {
        "input_schema": "get_input_jsonschema",
        "output_schema": "get_output_jsonschema",
        "config_schema": "get_config_jsonschema",
        "context_schema": "get_context_jsonschema",
    }.get(name)
    if method_name is not None:
        try:
            method = getattr(target, method_name, None)
            if callable(method):
                value = method()
                if isinstance(value, dict):
                    return dict(value)
        except Exception as exc:
            _LOGGER.debug("could not read LangGraph schema %s", method_name, exc_info=exc)
    if name == "state_schema":
        try:
            method = getattr(target, "get_input_jsonschema", None)
            if callable(method):
                value = method()
                if isinstance(value, dict):
                    return dict(value)
        except Exception as exc:
            _LOGGER.debug("could not read LangGraph state schema", exc_info=exc)
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


def _export_metadata(target: Any) -> dict[str, Any]:
    exported = getattr(target, "__universal_runtime__", None)
    if isinstance(exported, dict):
        return dict(exported)
    runtime_profile = getattr(target, "__runtime_profile__", None)
    if runtime_profile is not None:
        return {"profile": runtime_profile}
    config = getattr(target, "config", {})
    if isinstance(config, dict) and isinstance(config.get("metadata"), dict):
        return dict(config["metadata"])
    return {}


def _profile_from_metadata(metadata: dict[str, Any]) -> LangGraphProfile | None:
    value = metadata.get("profile", metadata.get("runtime_profile"))
    if value is None:
        integration = metadata.get("ls_integration")
        value = {
            "deepagents": LangGraphProfile.DEEPAGENTS,
            "langchain_create_agent": LangGraphProfile.LANGCHAIN_AGENT,
        }.get(str(integration))
    if value is None:
        return None
    try:
        return LangGraphProfile(str(value))
    except ValueError:
        return None


def is_compiled_graph(target: Any) -> bool:
    return hasattr(target, "astream") and hasattr(target, "ainvoke")
