from __future__ import annotations

from typing import Any

from langchain_core.messages import AIMessage, ToolMessage
from langgraph.graph import END, START, StateGraph

from phase1_agent.context import AgentState
from phase1_agent.tools import deterministic_weather


def agent_node(state: AgentState) -> dict[str, Any]:
    """Mock LLM decision: echo the user's text and emit one stable tool call."""
    messages = state.get("messages", [])
    user_text = ""
    if messages:
        user_text = str(getattr(messages[-1], "content", "") or "")
    return {
        "messages": [
            AIMessage(
                content=f"Mock cevap: {user_text}",
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


def build_graph(*, checkpointer: Any | None = None, store: Any | None = None) -> Any:
    builder = StateGraph(AgentState, context_schema=dict[str, str])
    builder.add_node("agent", agent_node)
    builder.add_node("tool", tool_node)
    builder.add_edge(START, "agent")
    builder.add_edge("agent", "tool")
    builder.add_edge("tool", END)
    compiled = builder.compile(checkpointer=checkpointer, store=store)
    compiled.__universal_runtime__ = {"profile": "langgraph"}
    return compiled


graph = build_graph()
