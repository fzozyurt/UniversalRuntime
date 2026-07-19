from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any, Protocol

from universal_runtime.domain.capabilities import AdapterCapabilities
from universal_runtime.domain.events import RuntimeEvent
from universal_runtime.domain.execution import ExecutionRequest


@dataclass(frozen=True, slots=True)
class AdapterManifest:
    adapter_id: str
    adapter_version: str
    profiles: frozenset[str]
    stream_modes: frozenset[str]
    capabilities: AdapterCapabilities
    custom_thread_id: bool = True
    custom_run_id: bool = True
    session_affinity: str = "none"

    @property
    def supported_profiles(self) -> tuple[str, ...]:
        return tuple(sorted(self.profiles))

    @property
    def supported_stream_modes(self) -> tuple[str, ...]:
        return tuple(sorted(self.stream_modes))


class RuntimeAdapter(Protocol):
    @property
    def manifest(self) -> AdapterManifest: ...

    async def inspect(self, target: Any) -> dict[str, Any]: ...

    async def invoke(self, request: ExecutionRequest) -> Any: ...

    def stream(self, request: ExecutionRequest) -> AsyncIterator[RuntimeEvent]: ...

    async def cancel(self, request: ExecutionRequest) -> None: ...
