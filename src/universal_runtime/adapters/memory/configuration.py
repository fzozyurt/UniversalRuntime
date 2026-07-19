from __future__ import annotations

import asyncio
import hashlib
import json
from copy import deepcopy
from dataclasses import replace

from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.domain.identity import ConfigRevisionId
from universal_runtime.domain.primitives.json_types import JsonObject
from universal_runtime.ports.configuration import ConfigRevision


class InMemoryApplicationConfigRepository:
    def __init__(self) -> None:
        self._revisions: dict[str, list[ConfigRevision]] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    def _lock(self, application_id: str) -> asyncio.Lock:
        return self._locks.setdefault(application_id, asyncio.Lock())

    async def get_active(self, application_id: str) -> ConfigRevision:
        async with self._lock(application_id):
            for revision in reversed(self._revisions.get(application_id, [])):
                if revision.active:
                    return deepcopy(revision)
        raise RuntimeFailure(
            ErrorCode.RESOURCE_NOT_FOUND, f"active config not found: {application_id}"
        )

    async def create_revision(self, application_id: str, config: JsonObject) -> ConfigRevision:
        async with self._lock(application_id):
            revisions = self._revisions.setdefault(application_id, [])
            canonical = json.dumps(
                config, sort_keys=True, separators=(",", ":"), ensure_ascii=False
            )
            revision = ConfigRevision(
                application_id,
                len(revisions) + 1,
                json.loads(canonical),
                hashlib.sha256(canonical.encode()).hexdigest(),
                not revisions,
                ConfigRevisionId.new(),
            )
            revisions.append(revision)
            return deepcopy(revision)

    async def activate(self, application_id: str, revision: int) -> ConfigRevision:
        async with self._lock(application_id):
            revisions = self._revisions.get(application_id, [])
            if revision < 1 or revision > len(revisions):
                raise RuntimeFailure(
                    ErrorCode.RESOURCE_NOT_FOUND, f"config revision not found: {revision}"
                )
            activated = [replace(item, active=item.revision == revision) for item in revisions]
            self._revisions[application_id] = activated
            return deepcopy(activated[revision - 1])
