from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Protocol

from universal_runtime.domain.identity import ConfigRevisionId
from universal_runtime.domain.primitives.json_types import JsonObject


@dataclass(frozen=True, slots=True)
class ConfigRevision:
    application_id: str
    revision: int
    config: JsonObject
    config_hash: str
    active: bool
    config_revision_id: ConfigRevisionId | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "config", deepcopy(self.config))


class ApplicationConfigRepository(Protocol):
    async def get_active(self, application_id: str) -> ConfigRevision: ...
    async def create_revision(self, application_id: str, config: JsonObject) -> ConfigRevision: ...
    async def activate(self, application_id: str, revision: int) -> ConfigRevision: ...
