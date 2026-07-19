from __future__ import annotations

from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.ports.runtime_adapter import RuntimeAdapter


class InMemoryAdapterRegistry:
    def __init__(self) -> None:
        self._adapters: dict[str, RuntimeAdapter] = {}

    def register(self, adapter: RuntimeAdapter) -> None:
        descriptor = getattr(adapter, "descriptor", None)
        adapter_id = str(getattr(descriptor, "graph_id", adapter.manifest.adapter_id))
        if adapter_id in self._adapters:
            raise RuntimeFailure(
                ErrorCode.INVALID_EXECUTION_INPUT, f"adapter already registered: {adapter_id}"
            )
        self._adapters[adapter_id] = adapter

    def get(self, adapter_id: str) -> RuntimeAdapter:
        try:
            return self._adapters[adapter_id]
        except KeyError as exc:
            raise RuntimeFailure(
                ErrorCode.ADAPTER_NOT_SUPPORTED, f"adapter not found: {adapter_id}"
            ) from exc

    def all(self) -> tuple[RuntimeAdapter, ...]:
        return tuple(self._adapters.values())
