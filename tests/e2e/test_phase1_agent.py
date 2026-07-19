from __future__ import annotations

import sys
from pathlib import Path

EXAMPLE_SRC = Path(__file__).parents[2] / "examples" / "phase1-agent" / "src"
sys.path.insert(0, str(EXAMPLE_SRC))


def test_deterministic_langgraph_tool_call() -> None:
    from phase1_agent.graph import graph

    result = graph.invoke({"messages": []})
    assert result["tool_result"] == "weather:Istanbul:sunny"


def test_langchain_agent_detects_as_langchain_profile() -> None:
    from phase1_agent.langchain_agent import agent

    from universal_runtime.adapters.langgraph.descriptor import LangGraphProfile
    from universal_runtime.adapters.langgraph.detector import detect_graph

    descriptor = detect_graph(agent, entrypoint="phase1_agent.langchain_agent:agent")
    assert descriptor.profile is LangGraphProfile.LANGCHAIN_AGENT
    assert descriptor.object_kind.value == "agent"


def test_langchain_agent_runs_without_credentials() -> None:
    from phase1_agent.langchain_agent import agent

    result = agent.invoke({"messages": [{"role": "user", "content": "weather?"}]})
    assert result["messages"][-1].content == "tool-result:weather:Istanbul:sunny"
