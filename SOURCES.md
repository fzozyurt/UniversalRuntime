# Primary Reference Sources

Implementers must verify current versions before pinning dependencies. Primary references used to shape this blueprint:

- LangGraph public repository, especially `libs/checkpoint`, `libs/checkpoint-postgres`, SDK clients and types.
- LangChain documentation for Agent Server API, threads, runs, streaming and persistence.
- A2A Protocol official specification and Agent Card/streaming documentation.
- OpenTelemetry official semantic conventions and zero-code instrumentation documentation.

Important current implementation note: explicit schema selection for Python `langgraph-checkpoint-postgres` must be verified against the installed version. If absent, use a dedicated role/pool with fixed `search_path`; never switch schemas dynamically on a shared pool.
