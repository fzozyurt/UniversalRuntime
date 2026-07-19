---
name: fastapi
description: FastAPI Custom HTTP skill for UniversalRuntime Phase 1.
---

# FastAPI Custom HTTP

Implement explicit/automatic detection, application mount/proxy, forwarded path handling, trace propagation and app migrations.

## Acceptance
- explicit entrypoint
- AST discovery
- root_path under Gateway prefix
- app schema migration isolation

## Engineering constraints

Follow root `AGENTS.md`. Update contracts and tests in the same change. Never claim completion before commands and outputs are recorded.
