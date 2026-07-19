# Phase 1 deterministic agent

This application is intentionally model-free. Its graph emits a LangChain
`AIMessage` tool call, executes `deterministic_weather`, and returns a
`ToolMessage`. It is suitable for image and detection smoke tests without API
credentials or network calls.

From the application directory:

```bash
uv run python -c "from phase1_agent.graph import graph; print(graph.invoke({'messages': []})['tool_result'])"
uv run universal-runtime inspect-graph --entrypoint phase1_agent.graph:graph
```
