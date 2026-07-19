from __future__ import annotations

from typing import Any

from langchain_core.messages import AIMessage, ToolMessage
from langgraph.graph import END, START, StateGraph

from phase1_agent.context import AgentState
from phase1_agent.tools import deterministic_weather


def agent_node(state: AgentState) -> dict[str, Any]:
    """Mock LLM decision: emit one stable tool call, with no external API."""
    del state
    return {
        "messages": [
            AIMessage(
                content="",
                tool_calls=[
                    {"name": "deterministic_weather", "args": {"city": "Istanbul"}, "id": "call-1"}
                ],
            )
        ]
    }


def tool_node(state: AgentState) -> dict[str, Any]:
    message = state["messages"][-1]
    result = deterministic_weather("Istanbul")
    return {
        "messages": [ToolMessage(content=result, tool_call_id=message.tool_calls[0]["id"])],
        "tool_result": result,
    }


def build_graph() -> Any:
    builder = StateGraph(AgentState, context_schema=dict[str, str])
    builder.add_node("agent", agent_node)
    builder.add_node("tool", tool_node)
    builder.add_edge(START, "agent")
    builder.add_edge("agent", "tool")
    builder.add_edge("tool", END)
    compiled = builder.compile()
    compiled.__universal_runtime__ = {"profile": "langgraph"}
    return compiled


graph = build_graph()
