from __future__ import annotations

from typing import Any, Protocol


class CustomHttpSurface(Protocol):
    async def descriptor(self) -> Any: ...
