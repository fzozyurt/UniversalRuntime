from __future__ import annotations

from typing import Any

from deepagents import create_deep_agent
from langchain_core.messages import AIMessage, BaseMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult

from phase1_agent.langchain_agent import DeterministicChatModel
from phase1_agent.tools import deterministic_weather


class TaskDelegatingChatModel(DeterministicChatModel):
    """Deterministic model that exercises Deep Agents' native ``task`` tool."""

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> ChatResult:
        del stop, run_manager, kwargs
        if messages and isinstance(messages[-1], ToolMessage):
            message = AIMessage(content=f"subagent-result:{messages[-1].content}")
        else:
            message = AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "task",
                        "args": {
                            "subagent_type": "weather-researcher",
                            "description": "Call deterministic_weather for Istanbul and return the exact result.",
                        },
                        "id": "task-call-1",
                    }
                ],
            )
        return ChatResult(generations=[ChatGeneration(message=message)])


deep_agent = create_deep_agent(
    model=TaskDelegatingChatModel(),
    name="phase1-deep-agent",
    system_prompt="You are a deterministic Deep Agent test harness.",
    tools=[deterministic_weather],
    subagents=[
        {
            "name": "weather-researcher",
            "description": "Research deterministic weather results.",
            "system_prompt": "Return the weather tool result concisely.",
            "model": DeterministicChatModel(),
            "tools": [deterministic_weather],
        }
    ],
)


def build_deep_agent(*, checkpointer: Any | None = None, store: Any | None = None) -> Any:
    """Build a persistence-injectable Deep Agent for the runtime adapter."""
    return create_deep_agent(
        model=TaskDelegatingChatModel(),
        name="phase1-deep-agent",
        system_prompt="You are a deterministic Deep Agent test harness.",
        tools=[deterministic_weather],
        subagents=[
            {
                "name": "weather-researcher",
                "description": "Research deterministic weather results.",
                "system_prompt": "Return the weather tool result concisely.",
                "model": DeterministicChatModel(),
                "tools": [deterministic_weather],
            }
        ],
        checkpointer=checkpointer,
        store=store,
    )
