from __future__ import annotations

import asyncio
from typing import TypedDict

import pytest
import uvicorn
from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt
from langgraph_sdk import get_client

from universal_runtime.adapters.langgraph import LangGraphAdapter
from universal_runtime.services.gateway.app import create_app


class CounterState(TypedDict, total=False):
    value: int


class ApprovalState(TypedDict, total=False):
    answer: str


def counter_graph() -> object:
    def increment(state: CounterState) -> dict[str, int]:
        return {"value": state.get("value", 0) + 1}

    builder = StateGraph(CounterState)
    builder.add_node("increment", increment)
    builder.add_edge(START, "increment")
    builder.add_edge("increment", END)
    return builder


def approval_graph() -> object:
    def ask(state: ApprovalState) -> dict[str, str]:
        return {"answer": interrupt({"question": "answer?"})}

    def finish(state: ApprovalState) -> dict[str, str]:
        return {"answer": state.get("answer", "") + "!"}

    builder = StateGraph(ApprovalState)
    builder.add_node("ask", ask)
    builder.add_node("finish", finish)
    builder.add_edge(START, "ask")
    builder.add_edge("ask", "finish")
    builder.add_edge("finish", END)
    return builder


async def _serve(adapter: LangGraphAdapter) -> tuple[uvicorn.Server, asyncio.Task[None], str]:
    server = uvicorn.Server(
        uvicorn.Config(create_app(runtime_adapter=adapter), host="127.0.0.1", port=0)
    )
    task = asyncio.create_task(server.serve())
    for _ in range(200):
        if server.started:
            break
        await asyncio.sleep(0.01)
    assert server.started
    socket = server.servers[0].sockets[0]
    return server, task, f"http://127.0.0.1:{socket.getsockname()[1]}"


async def _stop(server: uvicorn.Server, task: asyncio.Task[None]) -> None:
    server.should_exit = True
    await task


@pytest.mark.compatibility
@pytest.mark.asyncio
async def test_official_sdk_local_stream_state_history_and_v2() -> None:
    server, task, url = await _serve(
        LangGraphAdapter(counter_graph(), persistence_mode="platform-managed")
    )
    try:
        client = get_client(url=url)
        assistant = await client.assistants.create(graph_id="default")
        thread = await client.threads.create()
        events = [
            item
            async for item in client.runs.stream(
                thread["thread_id"],
                assistant["assistant_id"],
                input={"value": 1},
                stream_mode="values",
            )
        ]
        assert next(item.event for item in events) == "metadata"
        assert next(item.event for item in reversed(events)) == "end"
        assert {"value": 2} in [item.data for item in events if item.event == "values"]
        run_id = events[0].data["run_id"]
        joined = await client.runs.join(thread["thread_id"], run_id)
        assert joined["values"] == {"value": 2}
        run = await client.runs.get(thread["thread_id"], run_id)
        assert run["status"] == "success"
        state = await client.threads.get_state(thread["thread_id"])
        history = await client.threads.get_history(thread["thread_id"])
        assert state["values"] == {"value": 2}
        assert history
        await client.runs.cancel(thread["thread_id"], run_id, wait=True)

        v2_thread = await client.threads.create()
        v2 = [
            item
            async for item in client.runs.stream(
                v2_thread["thread_id"],
                assistant["assistant_id"],
                input={"value": 2},
                stream_mode="values",
                version="v2",
            )
        ]
        assert all("type" in item for item in v2)
    finally:
        await _stop(server, task)


@pytest.mark.compatibility
@pytest.mark.asyncio
async def test_official_sdk_local_interrupt_resume_and_cancel() -> None:
    server, task, url = await _serve(
        LangGraphAdapter(approval_graph(), persistence_mode="platform-managed")
    )
    try:
        client = get_client(url=url)
        assistant = await client.assistants.create(graph_id="default")
        thread = await client.threads.create()
        first = [
            item
            async for item in client.runs.stream(
                thread["thread_id"], assistant["assistant_id"], input={}, stream_mode="values"
            )
        ]
        assert any("__interrupt__" in (item.data or {}) for item in first if item.event == "values")
        resumed = [
            item
            async for item in client.runs.stream(
                thread["thread_id"],
                assistant["assistant_id"],
                command={"resume": "yes"},
                stream_mode="values",
            )
        ]
        assert resumed[-1].event == "end"
        state = await client.threads.get_state(thread["thread_id"])
        assert state["values"] == {"answer": "yes!"}
        history = await client.threads.get_history(thread["thread_id"])
        assert len(history) >= 3
    finally:
        await _stop(server, task)
