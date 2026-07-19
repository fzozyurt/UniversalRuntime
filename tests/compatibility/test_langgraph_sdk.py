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
