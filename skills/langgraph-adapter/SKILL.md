---
name: langgraph-adapter
description: LangGraph Adapter skill for UniversalRuntime Phase 1.
---

# LangGraph Adapter

Implement detection, loading, config/context injection, managed persistence, stream conversion and capability operations for LangGraph, LangChain create_agent and Deep Agents.

## Acceptance
- official langgraph_sdk tests
- InMemory and PostgreSQL persistence
- interrupt/resume
- subgraph namespaces/tool events
- no private mutation without isolated shim test

## Engineering constraints

Follow root `AGENTS.md`. Update contracts and tests in the same change. Never claim completion before commands and outputs are recorded.
