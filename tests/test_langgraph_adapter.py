from __future__ import annotations

import asyncio
from typing import TypedDict

import pytest
from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt

from universal_runtime.adapters.langgraph import LangGraphAdapter
from universal_runtime.adapters.langgraph.config_mapper import map_config
from universal_runtime.adapters.langgraph.descriptor import GraphObjectKind, LangGraphProfile
from universal_runtime.adapters.langgraph.detector import detect_graph
from universal_runtime.adapters.langgraph.errors import LangGraphAdapterError, LangGraphErrorCode
from universal_runtime.adapters.langgraph.stream_mapper import map_stream
from universal_runtime.domain.events import RuntimeEventType
from universal_runtime.domain.execution import ExecutionRequest
from universal_runtime.domain.identity import (
    ApplicationId,
    ApplicationScope,
    AssistantId,
    AttemptId,
    DeploymentId,
    ProjectId,
    RevisionId,
    RunId,
    ThreadId,
    WorkspaceId,
)


class State(TypedDict, total=False):
    count: int
    answer: str


def identity(run: str = "run", thread: str | None = "thread"):
    return __import__(
        "universal_runtime.domain.identity", fromlist=["ExecutionIdentity"]
    ).ExecutionIdentity(
        ApplicationScope(
            WorkspaceId.parse("workspace"),
            ProjectId.parse("project"),
            ApplicationId.parse("application"),
            RevisionId.parse("revision"),
            DeploymentId.parse("deployment"),
        ),
        AssistantId.parse("assistant"),
        RunId.parse(run),
        AttemptId.parse("attempt"),
        ThreadId.parse(thread) if thread is not None else None,
    )


def builder() -> StateGraph:
    graph = StateGraph(State)
    graph.add_node("increment", lambda state: {"count": state.get("count", 0) + 1})
    graph.add_edge(START, "increment")
    graph.add_edge("increment", END)
    return graph


def request(**kwargs: object) -> ExecutionRequest:
    return ExecutionRequest(identity(), input=kwargs or {"count": 1})


def test_compiled_and_factory_detection() -> None:
    compiled = builder().compile()
    descriptor = detect_graph(compiled, entrypoint="app:graph")
    assert descriptor.object_kind is GraphObjectKind.COMPILED
    assert descriptor.profile is LangGraphProfile.LANGGRAPH
    factory_descriptor = detect_graph(builder)
    assert factory_descriptor.object_kind is GraphObjectKind.FACTORY


@pytest.mark.asyncio
async def test_factory_injects_managed_memory_and_preserves_config() -> None:
    adapter = LangGraphAdapter(builder, persistence_mode="platform-managed")
    mapped = map_config(
        ExecutionRequest(
            identity(),
            input={"count": 1},
            config={
                "tags": ["caller"],
                "configurable": {"thread_id": "attacker"},
                "metadata": {"x": 1},
            },
            context={"tenant": "acme"},
        )
    )
    assert mapped["configurable"]["thread_id"] == "thread"
    assert mapped["metadata"]["runtime.run_id"] == "run"
    assert mapped["tags"] == ["caller"]
    assert (await adapter.invoke(request(count=1)))["count"] == 2
    assert adapter.manifest.adapter_id == "langgraph"


@pytest.mark.asyncio
async def test_values_and_updates_stream_are_canonicalized() -> None:
    adapter = LangGraphAdapter(builder)
    events = [
        event
        async for event in adapter.stream(
            ExecutionRequest(identity(), input={"count": 1}, stream_modes=("values", "updates"))
        )
    ]
    event_types = [event.type for event in events]
    assert RuntimeEventType.STATE_VALUES in event_types
    assert RuntimeEventType.STATE_UPDATES in event_types


@pytest.mark.asyncio
async def test_state_history_and_update_use_same_thread() -> None:
    adapter = LangGraphAdapter(builder, persistence_mode="platform-managed")
    run_request = ExecutionRequest(identity("state-run"), input={"count": 1})
    await adapter.invoke(run_request)
    state = await adapter.get_state(run_request)
    assert state.values["count"] == 2
    history = await adapter.get_state_history(run_request)
    assert history
    updated = await adapter.update_state(run_request, {"count": 10})
    assert updated["configurable"]["thread_id"] == "thread"


def test_protected_runtime_values_cannot_be_overridden() -> None:
    mapped = map_config(
        ExecutionRequest(
            identity(), config={"run_id": "bad", "metadata": {"runtime.run_id": "bad"}}
        )
    )
    assert mapped["run_id"] == "run"
    assert mapped["metadata"]["runtime.run_id"] == "run"


@pytest.mark.asyncio
async def test_interrupt_and_resume_stays_on_thread() -> None:
    graph = StateGraph(State)
    graph.add_node("ask", lambda state: {"answer": interrupt({"question": "answer?"})})
    graph.add_edge(START, "ask")
    graph.add_edge("ask", END)
    adapter = LangGraphAdapter(graph, persistence_mode="platform-managed")
    run_request = ExecutionRequest(identity("interrupt-run"), input={})
    interrupted = await adapter.invoke(run_request)
    assert "__interrupt__" in interrupted
    resumed = await adapter.resume(run_request, "yes")
    assert resumed["answer"] == "yes"


@pytest.mark.asyncio
async def test_stream_v2_tool_and_subgraph_namespace_mapping() -> None:
    async def chunks():
        yield {"type": "custom", "ns": ["supervisor", "research-agent"], "data": {"step": 1}}
        yield ("events", {"event": "on_tool_start", "name": "search"})
        yield ("events", {"event": "on_tool_end", "name": "search"})

    events = [event async for event in map_stream(chunks(), request())]
    assert events[0].type is RuntimeEventType.CUSTOM
    assert events[0].namespace == ("supervisor", "research-agent")
    assert events[1].type is RuntimeEventType.TOOL_STARTED
    assert events[2].type is RuntimeEventType.TOOL_COMPLETED


@pytest.mark.asyncio
async def test_builder_receives_managed_checkpointer_and_store() -> None:
    captured: dict[str, object] = {}

    def factory(*, checkpointer=None, store=None):
        captured["checkpointer"] = checkpointer
        captured["store"] = store
        return builder().compile(checkpointer=checkpointer, store=store)

    adapter = LangGraphAdapter(factory, persistence_mode="platform-managed")
    assert captured["checkpointer"] is not None
    assert captured["store"] is not None
    assert adapter.manifest.capabilities.checkpoint is True


@pytest.mark.asyncio
async def test_compiled_graph_rejects_platform_managed_persistence() -> None:
    compiled = builder().compile()
    with pytest.raises(LangGraphAdapterError) as error:
        LangGraphAdapter(compiled, persistence_mode="platform-managed")
    assert error.value.code is LangGraphErrorCode.INVALID_PERSISTENCE


@pytest.mark.asyncio
async def test_application_managed_compiled_graph_preserves_persistence() -> None:
    from langgraph.checkpoint.memory import InMemorySaver

    compiled = builder().compile(checkpointer=InMemorySaver())
    adapter = LangGraphAdapter(compiled, persistence_mode="application-managed")
    assert adapter.manifest.capabilities.checkpoint is True
    assert adapter.manifest.session_affinity.value == "required"


async def _consume(stream):
    async for _ in stream:
        pass


@pytest.mark.asyncio
async def test_cancel_stops_registered_stream_task() -> None:
    started = asyncio.Event()

    async def blocking_node(state: State):
        started.set()
        await asyncio.Future()
        return state

    graph = StateGraph(State)
    graph.add_node("blocking", blocking_node)
    graph.add_edge(START, "blocking")
    graph.add_edge("blocking", END)
    adapter = LangGraphAdapter(graph)
    run_request = ExecutionRequest(identity("cancel-run"), input={})
    stream_task = asyncio.create_task(_consume(adapter.stream(run_request)))
    await started.wait()
    await adapter.cancel(run_request)
    with pytest.raises(asyncio.CancelledError):
        await stream_task


@pytest.mark.asyncio
async def test_unsupported_state_is_typed() -> None:
    adapter = LangGraphAdapter(builder)
    with pytest.raises(LangGraphAdapterError) as error:
        await adapter.get_state(request())
    assert error.value.code is LangGraphErrorCode.CAPABILITY_NOT_SUPPORTED


def test_langchain_and_deepagents_profiles_are_detected() -> None:
    from deepagents import create_deep_agent
    from langchain.agents import create_agent
    from langchain_core.language_models.fake_chat_models import GenericFakeChatModel

    model = GenericFakeChatModel(messages=iter(["ok"]))
    langchain_graph = create_agent(model, name="langchain-demo")
    deep_graph = create_deep_agent(model=model, name="deep-demo")
    assert detect_graph(langchain_graph).profile is LangGraphProfile.LANGCHAIN_AGENT
    assert detect_graph(deep_graph).profile is LangGraphProfile.DEEPAGENTS


def explicit_factory(*, checkpointer=None, store=None):
    del checkpointer, store
    return builder()


@pytest.mark.asyncio
async def test_explicit_entrypoint_factory_loading() -> None:
    from universal_runtime.adapters.langgraph.loader import load_entrypoint

    target = await __import__("asyncio").to_thread(load_entrypoint, f"{__name__}:explicit_factory")
    assert hasattr(target, "ainvoke")


@pytest.mark.asyncio
async def test_context_is_passed_as_runtime_context() -> None:
    from langgraph.runtime import Runtime

    class Context(TypedDict):
        tenant: str

    def contextual_node(state: State, runtime: Runtime[Context]):
        return {"answer": runtime.context["tenant"]}

    graph = StateGraph(State, context_schema=Context)
    graph.add_node("context", contextual_node)
    graph.add_edge(START, "context")
    graph.add_edge("context", END)
    adapter = LangGraphAdapter(graph)
    result = await adapter.invoke(
        ExecutionRequest(identity(), input={}, context={"tenant": "acme"})
    )
    assert result["answer"] == "acme"


@pytest.mark.asyncio
async def test_langchain_agent_output_runs_through_same_adapter() -> None:
    from langchain.agents import create_agent
    from langchain_core.language_models.fake_chat_models import GenericFakeChatModel

    graph = create_agent(
        GenericFakeChatModel(messages=iter(["deterministic answer"])), name="agent"
    )
    adapter = LangGraphAdapter(graph)
    result = await adapter.invoke(
        ExecutionRequest(
            identity("agent-run"), input={"messages": [{"role": "user", "content": "hi"}]}
        )
    )
    assert result["messages"]
    assert adapter.descriptor.profile is LangGraphProfile.LANGCHAIN_AGENT
