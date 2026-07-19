from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class ConfigRevision:
    application_id: str
    revision: int
    config: dict[str, Any]
    config_hash: str
    active: bool


class ApplicationConfigRepository(Protocol):
    async def get_active(self, application_id: str) -> ConfigRevision: ...

    async def create_revision(
        self, application_id: str, config: dict[str, Any]
    ) -> ConfigRevision: ...

    async def activate(self, application_id: str, revision: int) -> ConfigRevision: ...
