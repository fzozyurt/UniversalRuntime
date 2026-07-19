# Official LangGraph SDK Compatibility Suite

Tests must instantiate the official SDK client against a live ASGI server or HTTP process.

Required fixtures:

- basic graph,
- interrupt graph,
- subgraph/custom event graph,
- LangChain `create_agent` graph with fake model,
- Deep Agents graph or a controlled integration fixture.

Golden fixtures capture:

- request JSON,
- raw SSE lines,
- SDK-decoded v1/v2 parts,
- canonical internal events.

Never assert only on status code. Validate IDs, ordering, namespace, metadata and final state.
