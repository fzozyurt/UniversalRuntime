from __future__ import annotations

import secrets
import time
from dataclasses import dataclass
from typing import Self
from uuid import UUID


def _generate_uuid7() -> str:
    """Generate RFC 9562 UUIDv7 without a version-dependent fallback."""
    timestamp_ms = int(time.time() * 1000) & ((1 << 48) - 1)
    random_a = secrets.randbits(12)
    random_b = secrets.randbits(62)
    value = (timestamp_ms << 80) | (0x7 << 76) | (random_a << 64) | (0x2 << 62) | random_b
    return str(UUID(int=value))


@dataclass(frozen=True, slots=True, eq=False)
class TypedId:
    value: str

    def __post_init__(self) -> None:
        if not self.value:
            raise ValueError("identifier must not be empty")

    @classmethod
    def parse(cls, value: str) -> Self:
        return cls(value)

    @classmethod
    def new(cls) -> Self:
        return cls(_generate_uuid7())

    def __str__(self) -> str:
        return self.value

    def __eq__(self, other: object) -> bool:
        return self.value == (other.value if isinstance(other, TypedId) else other)

    def __hash__(self) -> int:
        return hash((type(self), self.value))


class WorkspaceId(TypedId):
    pass


class ProjectId(TypedId):
    pass


class ApplicationId(TypedId):
    pass


class RevisionId(TypedId):
    pass


class DeploymentId(TypedId):
    pass


class AssistantId(TypedId):
    pass


class ThreadId(TypedId):
    pass


class RunId(TypedId):
    pass


class AttemptId(TypedId):
    pass


class EventId(TypedId):
    pass


class WorkerId(TypedId):
    pass


class CommandId(TypedId):
    pass


class LeaseId(TypedId):
    pass


class ConfigRevisionId(TypedId):
    pass
