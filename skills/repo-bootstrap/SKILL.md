---
name: repo-bootstrap
description: Repository Bootstrap skill for UniversalRuntime Phase 1.
---

# Repository Bootstrap

Create the repository structure, packaging, quality tooling and architecture guard before feature code.

## Deliverables
- pyproject, Makefile, gitignore, CI
- package/service directories
- architecture guard and contract validator

## Acceptance
- editable install works
- empty tests run
- Ruff/mypy/architecture/contract validation pass
- no framework imports in domain

## Engineering constraints

Follow root `AGENTS.md`. Update contracts and tests in the same change. Never claim completion before commands and outputs are recorded.
