# LangGraph Adapter Implementation Checklist

The adapter must:

1. detect compiled LangGraph/Pregel-compatible objects,
2. treat LangChain `create_agent` and Deep Agents outputs as LangGraph profiles,
3. load builder/factory entrypoints and compile using managed saver/store,
4. inject protected `thread_id`, `run_id`, assistant and deployment metadata,
5. pass modern `context` separately,
6. convert native stream parts to canonical events with namespace,
7. expose state/history/update/interrupt only when the graph supports them,
8. preserve native payload in `RuntimeEvent.native`,
9. avoid private attribute mutation unless pinned, tested and isolated behind a compatibility shim,
10. maintain golden tests against the official `langgraph_sdk`.

## Deterministic verification

`tests/test_langgraph_adapter.py` covers compiled graphs, builder/factory loading, managed local saver/store, protected configuration, separate context, values/updates/v2 stream parts, tool and subgraph namespaces, state/history/update, interrupt/resume, LangChain `create_agent` and Deep Agents profile detection.

`tests/compatibility/test_langgraph_sdk.py` is the official SDK smoke surface. It runs when `UR_LANGGRAPH_SDK_URL` points at a running Gateway and is skipped otherwise.

PostgreSQL saver/store wiring remains a provider boundary; the local profile uses upstream `InMemorySaver` and `InMemoryStore`, and no upstream checkpoint schema is forked.
