from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any, Protocol

from universal_runtime.domain.capabilities import AdapterManifest
from universal_runtime.domain.events import RuntimeEvent
from universal_runtime.domain.execution import ExecutionRequest


class RuntimeAdapter(Protocol):
    @property
    def manifest(self) -> AdapterManifest: ...

    async def inspect(self, target: Any) -> dict[str, Any]: ...

    async def invoke(self, request: ExecutionRequest) -> Any: ...

    def stream(self, request: ExecutionRequest) -> AsyncIterator[RuntimeEvent]: ...

    async def cancel(self, request: ExecutionRequest) -> None: ...
