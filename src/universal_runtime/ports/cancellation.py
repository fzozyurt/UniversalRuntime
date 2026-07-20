from __future__ import annotations

from typing import Protocol

from universal_runtime.domain.execution import Run


class RunCancellation(Protocol):
    async def cancel(self, run: Run) -> bool:
        """Request cancellation from the execution owner.

        Returns ``True`` when an active execution owner accepted the request and
        ``False`` when the run had not yet been leased or was no longer active.
        """
        ...
