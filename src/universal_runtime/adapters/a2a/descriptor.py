from __future__ import annotations

from universal_runtime.domain.capabilities import AdapterManifest
from universal_runtime.ports.agent_discovery import AgentCardDescriptor, AgentSkillDescriptor


def descriptor_from_manifest(
    *,
    name: str,
    description: str,
    version: str,
    url: str,
    manifest: AdapterManifest,
) -> AgentCardDescriptor:
    capabilities = manifest.capabilities
    skills: list[AgentSkillDescriptor] = [
        AgentSkillDescriptor(
            skill_id="runtime.execute",
            name="Execute",
            description="Execute the configured assistant.",
            tags=("execution",),
        )
    ]
    if capabilities.streaming and capabilities.a2a:
        skills.append(
            AgentSkillDescriptor(
                skill_id="runtime.stream",
                name="Stream execution",
                description="Stream ordered execution updates.",
                tags=("streaming",),
            )
        )
    return AgentCardDescriptor(
        name=name,
        description=description,
        version=version,
        url=url,
        streaming=capabilities.streaming and capabilities.a2a,
        skills=tuple(skills),
    )
