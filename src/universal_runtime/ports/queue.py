from __future__ import annotations

from typing import Protocol

from universal_runtime.domain.execution import ExecutionRequest


class RunCommandQueue(Protocol):
    async def publish(self, request: ExecutionRequest) -> None: ...

    async def receive(self) -> ExecutionRequest: ...

    async def acknowledge(self, request: ExecutionRequest) -> None: ...

    async def reject(self, request: ExecutionRequest, *, retryable: bool) -> None: ...
