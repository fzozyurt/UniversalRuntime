from __future__ import annotations

from typing import Protocol

from universal_runtime.ports.runtime_adapter import RuntimeAdapter


class AdapterRegistry(Protocol):
    def register(self, adapter: RuntimeAdapter) -> None: ...

    def get(self, adapter_id: str) -> RuntimeAdapter: ...

    def all(self) -> tuple[RuntimeAdapter, ...]: ...
