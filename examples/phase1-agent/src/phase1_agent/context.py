from __future__ import annotations

from typing import TypedDict


class AgentState(TypedDict, total=False):
    messages: list[object]
    tool_result: str


class RuntimeContext(TypedDict, total=False):
    user: str
    request_id: str
