---
name: a2a
description: A2A Adapter skill for UniversalRuntime Phase 1.
---

# A2A Adapter

Generate Agent Card from assistant metadata and map A2A context/task/message/artifact/streaming to UniversalRuntime identities and events.

## Acceptance
- discovery endpoint
- streaming advertised only when implemented
- task/run and context/thread mapping
- typed unsupported capability response

## Engineering constraints

Follow root `AGENTS.md`. Update contracts and tests in the same change. Never claim completion before commands and outputs are recorded.
