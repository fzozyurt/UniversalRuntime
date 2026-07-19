from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class AgentSkillDescriptor:
    skill_id: str
    name: str
    description: str
    tags: tuple[str, ...] = ()
    input_modes: tuple[str, ...] = ("text",)
    output_modes: tuple[str, ...] = ("text",)


@dataclass(frozen=True, slots=True)
class AgentCardDescriptor:
    name: str
    description: str
    version: str
    url: str
    streaming: bool
    skills: tuple[AgentSkillDescriptor, ...]


class AgentDiscovery(Protocol):
    async def describe(self) -> AgentCardDescriptor: ...
