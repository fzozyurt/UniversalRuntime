from __future__ import annotations

import os

import pytest


@pytest.mark.compatibility
@pytest.mark.skipif(
    not os.getenv("UR_LANGGRAPH_SDK_URL"),
    reason="requires a running LangGraph-compatible gateway",
)
@pytest.mark.asyncio
async def test_official_langgraph_sdk_assistant_thread_run_surface() -> None:
    from langgraph_sdk import get_client

    client = get_client(url=os.environ["UR_LANGGRAPH_SDK_URL"])
    assistant = await client.assistants.create(graph_id="default")
    thread = await client.threads.create()
    run = await client.runs.create(thread["thread_id"], assistant["assistant_id"], input={})
    events = [event async for event in client.runs.stream(thread["thread_id"], run["run_id"])]
    assert events


@pytest.mark.compatibility
@pytest.mark.skipif(
    not os.getenv("UR_LANGGRAPH_SDK_URL"),
    reason="requires a running LangGraph-compatible gateway",
)
@pytest.mark.asyncio
async def test_official_langgraph_sdk_state_history_update_and_cancel() -> None:
    from langgraph_sdk import get_client

    client = get_client(url=os.environ["UR_LANGGRAPH_SDK_URL"])
    assistant = await client.assistants.create(graph_id="default")
    thread = await client.threads.create()
    run = await client.runs.create(
        thread["thread_id"],
        assistant["assistant_id"],
        input={},
        stream_mode="values",
    )
    state = await client.threads.get_state(thread["thread_id"])
    history = await client.threads.get_history(thread["thread_id"])
    assert state is not None
    assert history
    await client.threads.update_state(thread["thread_id"], {})
    await client.runs.cancel(thread["thread_id"], run["run_id"], wait=True)
