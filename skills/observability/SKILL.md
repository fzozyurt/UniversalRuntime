---
name: observability
description: Observability skill for UniversalRuntime Phase 1.
---

# Observability

Implement OTel bootstrap, optional OpenLIT adapter, HTTP/worker/gRPC/Kafka/DB trace propagation and secret-safe exception capture.

## Acceptance
- disabled path adds no exporter requirement
- enabled launcher uses zero-code instrumentation
- background runtime.run span
- exception and trace correlation

## Engineering constraints

Follow root `AGENTS.md`. Update contracts and tests in the same change. Never claim completion before commands and outputs are recorded.
