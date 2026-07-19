from __future__ import annotations

from typing import Any

from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.ports.agent_discovery import AgentCardDescriptor


def build_agent_card(descriptor: AgentCardDescriptor) -> Any:
    """Build the pinned SDK's protobuf AgentCard without leaking SDK types inward."""
    try:
        from a2a.types import AgentCard, AgentInterface, AgentSkill
    except ImportError as exc:  # pragma: no cover - exercised in minimal installs
        raise RuntimeFailure(
            ErrorCode.ADAPTER_NOT_SUPPORTED,
            "A2A adapter is not installed; install the a2a extra",
        ) from exc

    card = AgentCard(
        name=descriptor.name,
        description=descriptor.description,
        version=descriptor.version,
        supported_interfaces=[
            AgentInterface(
                url=descriptor.url,
                protocol_binding="HTTP+JSON",
                protocol_version="1.0",
            )
        ],
        capabilities={"streaming": descriptor.streaming},
        default_input_modes=["text", "application/json"],
        default_output_modes=["text", "application/json"],
    )
    card.skills.extend(
        AgentSkill(
            id=skill.skill_id,
            name=skill.name,
            description=skill.description,
            tags=list(skill.tags),
            input_modes=list(skill.input_modes),
            output_modes=list(skill.output_modes),
        )
        for skill in descriptor.skills
    )
    return card
