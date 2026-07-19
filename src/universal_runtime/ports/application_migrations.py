from __future__ import annotations

from typing import Protocol


class ApplicationMigrationRunner(Protocol):
    async def upgrade(self, *, config_path: str, application_id: str, environment: str) -> None: ...
