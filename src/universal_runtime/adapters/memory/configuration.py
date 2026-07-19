from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from typing import Any

from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.ports.configuration import ConfigRevision


class InMemoryApplicationConfigRepository:
    def __init__(self) -> None:
        self._revisions: dict[str, list[ConfigRevision]] = {}

    async def get_active(self, application_id: str) -> ConfigRevision:
        revisions = self._revisions.get(application_id, [])
        for revision in reversed(revisions):
            if revision.active:
                return revision
        raise RuntimeFailure(
            ErrorCode.RESOURCE_NOT_FOUND, f"active config not found: {application_id}"
        )

    async def create_revision(self, application_id: str, config: dict[str, Any]) -> ConfigRevision:
        revisions = self._revisions.setdefault(application_id, [])
        canonical = json.dumps(config, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        revision = ConfigRevision(
            application_id=application_id,
            revision=len(revisions) + 1,
            config=json.loads(canonical),
            config_hash=hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
            active=not revisions,
        )
        revisions.append(revision)
        return revision

    async def activate(self, application_id: str, revision: int) -> ConfigRevision:
        revisions = self._revisions.get(application_id, [])
        if revision < 1 or revision > len(revisions):
            raise RuntimeFailure(
                ErrorCode.RESOURCE_NOT_FOUND, f"config revision not found: {revision}"
            )
        activated = [replace(item, active=item.revision == revision) for item in revisions]
        self._revisions[application_id] = activated
        return activated[revision - 1]
