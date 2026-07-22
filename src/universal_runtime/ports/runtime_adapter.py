from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any, Protocol

from universal_runtime.domain.capabilities import AdapterManifest
from universal_runtime.domain.events import RuntimeEventDraft
from universal_runtime.domain.execution import ExecutionRequest


class RuntimeAdapter(Protocol):
    @property
    def manifest(self) -> AdapterManifest: ...

    async def inspect(self, target: Any) -> dict[str, Any]: ...

    async def invoke(self, request: ExecutionRequest) -> Any: ...

    def stream(self, request: ExecutionRequest) -> AsyncIterator[RuntimeEventDraft]: ...

    async def cancel(self, request: ExecutionRequest) -> None: ...

    async def get_state(self, request: ExecutionRequest) -> Any | None:
        """Return the current framework-native state for the thread, or None."""

    async def get_state_history(self, request: ExecutionRequest) -> Any | None:
        """Return framework-native state history for the thread, or None."""
