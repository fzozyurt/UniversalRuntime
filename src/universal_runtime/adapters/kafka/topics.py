from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TopicNames:
    interactive: str
    normal: str
    batch: str
    execution_events: str
    lifecycle: str
    commands: str
    audit: str
    deadletter: str

    @classmethod
    def from_config(
        cls,
        *,
        prefix: str = "rt",
        environment: str = "local",
        overrides: Mapping[str, str] | None = None,
    ) -> TopicNames:
        if not prefix or not environment:
            raise ValueError("topic prefix and environment must not be empty")
        root = f"{prefix}.{environment}"
        defaults = {
            "interactive": f"{root}.runs.interactive.v1",
            "normal": f"{root}.runs.normal.v1",
            "batch": f"{root}.runs.batch.v1",
            "execution_events": f"{root}.execution.events.v1",
            "lifecycle": f"{root}.run.lifecycle.v1",
            "commands": f"{root}.run.commands.v1",
            "audit": f"{root}.audit.events.v1",
            "deadletter": f"{root}.deadletter.v1",
        }
        if overrides:
            unknown = set(overrides).difference(defaults)
            if unknown:
                names = ", ".join(sorted(unknown))
                raise ValueError(f"unknown topic override(s): {names}")
            defaults.update(overrides)
        return cls(**defaults)

    def as_dict(self) -> dict[str, str]:
        return {
            "interactive": self.interactive,
            "normal": self.normal,
            "batch": self.batch,
            "execution_events": self.execution_events,
            "lifecycle": self.lifecycle,
            "commands": self.commands,
            "audit": self.audit,
            "deadletter": self.deadletter,
        }
