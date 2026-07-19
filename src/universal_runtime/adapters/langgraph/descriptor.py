from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from universal_runtime.domain.primitives.json_types import JsonObject


class LangGraphProfile(StrEnum):
    LANGGRAPH = "langgraph"
    LANGCHAIN_AGENT = "langchain-agent"
    DEEPAGENTS = "deepagents"


class GraphObjectKind(StrEnum):
    COMPILED = "compiled"
    FACTORY = "factory"
    BUILDER = "builder"
    AGENT = "agent"


@dataclass(frozen=True, slots=True)
class LangGraphDescriptor:
    profile: LangGraphProfile
    entrypoint: str
    object_kind: GraphObjectKind
    graph_id: str
    has_checkpointer: bool
    has_store: bool
    input_schema: JsonObject | None = None
    output_schema: JsonObject | None = None
    state_schema: JsonObject | None = None
    config_schema: JsonObject | None = None
    context_schema: JsonObject | None = None
