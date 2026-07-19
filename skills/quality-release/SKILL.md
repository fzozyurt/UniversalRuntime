---
name: quality-release
description: Quality and Release skill for UniversalRuntime Phase 1.
---

# Quality and Release

Enforce Ruff, strict mypy, architecture guard, contract validation, >=80 coverage, compatibility matrix and release notes.

## Acceptance
- zero unexplained skips in service-backed CI
- package/container build
- documented supported SDK versions
- draft PR checklist complete

## Engineering constraints

Follow root `AGENTS.md`. Update contracts and tests in the same change. Never claim completion before commands and outputs are recorded.
