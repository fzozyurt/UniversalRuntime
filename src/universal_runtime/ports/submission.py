from __future__ import annotations

from typing import Protocol

from universal_runtime.domain.execution import Run, RunCommand, Thread


class RunSubmissionStore(Protocol):
    async def submit(
        self,
        run: Run,
        command: RunCommand,
        *,
        thread: Thread | None,
    ) -> Run:
        """Persist run, thread transition and command intent atomically."""
        ...
