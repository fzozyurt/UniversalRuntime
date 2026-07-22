from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from universal_runtime.domain.execution.priority import QueuePriority


@dataclass(frozen=True, slots=True)
class TopicNames:
    short_queue: str
    long_queue: str
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
            "short_queue": f"{root}.runs.short_queue",
            "long_queue": f"{root}.runs.long_queue",
            "execution_events": f"{root}.execution.events",
            "lifecycle": f"{root}.run.lifecycle",
            "commands": f"{root}.run.commands",
            "audit": f"{root}.audit.events",
            "deadletter": f"{root}.deadletter",
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
            "short_queue": self.short_queue,
            "long_queue": self.long_queue,
            "execution_events": self.execution_events,
            "lifecycle": self.lifecycle,
            "commands": self.commands,
            "audit": self.audit,
            "deadletter": self.deadletter,
        }

    @staticmethod
    def run_topic_for(
        prefix: str,
        application_id: str,
        priority: int = 100,
    ) -> str:
        queue = "short_queue" if priority >= QueuePriority.NORMAL else "long_queue"
        return f"{prefix}.{application_id}.runs.{queue}"
