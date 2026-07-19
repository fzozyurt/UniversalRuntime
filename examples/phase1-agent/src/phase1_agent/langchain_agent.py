from __future__ import annotations

from typing import Any

from langchain.agents import create_agent
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.runnables import RunnableBinding

from phase1_agent.tools import deterministic_weather


class DeterministicChatModel(BaseChatModel):
    """A model-free ChatModel that deterministically exercises one tool call."""

    @property
    def _llm_type(self) -> str:
        return "phase1-deterministic"

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> ChatResult:
        del stop, run_manager, kwargs
        if messages and isinstance(messages[-1], ToolMessage):
            message: AIMessage = AIMessage(content=f"tool-result:{messages[-1].content}")
        else:
            message = AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "deterministic_weather",
                        "args": {"city": "Istanbul"},
                        "id": "lc-call-1",
                    }
                ],
            )
        return ChatResult(generations=[ChatGeneration(message=message)])

    def bind_tools(self, tools: Any, **kwargs: Any) -> RunnableBinding:
        return RunnableBinding(bound=self, kwargs={"tools": tools, **kwargs})


agent = create_agent(
    DeterministicChatModel(),
    tools=[deterministic_weather],
    name="phase1-langchain-agent",
)
