from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

from universal_runtime.domain.execution.priority import QueuePriority


@dataclass(frozen=True, slots=True)
class TopicNames:
    """Canonical Kafka topic contract.

    Run topics are owned by an application deployment and consumed directly by
    that application's worker consumer group. There is no dispatcher topic or
    dispatcher service in the routing path.
    """

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
        application_id: str = "default",
        overrides: Mapping[str, str] | None = None,
    ) -> TopicNames:
        if not prefix or not environment or not application_id:
            raise ValueError("topic prefix, environment and application_id must not be empty")
        root = cls.application_root(prefix, environment, application_id)
        defaults = {
            "short_queue": f"{root}.runs.short_queue.v1",
            "long_queue": f"{root}.runs.long_queue.v1",
            "execution_events": f"{root}.execution.events.v1",
            "lifecycle": f"{root}.run.lifecycle.v1",
            "commands": f"{root}.run.commands.v1",
            "audit": f"{root}.audit.events.v1",
            "deadletter": f"{root}.deadletter.v1",
        }
        env_overrides = {}
        for role in defaults:
            val = os.environ.get(f"UR_TOPIC_{role.upper()}")
            if val:
                env_overrides[role] = val
        if overrides:
            unknown = set(overrides).difference(defaults)
            if unknown:
                names = ", ".join(sorted(unknown))
                raise ValueError(f"unknown topic override(s): {names}")
            defaults.update(overrides)
        if env_overrides:
            defaults.update(env_overrides)
        return cls(**defaults)

    @staticmethod
    def application_root(prefix: str, environment: str, application_id: str) -> str:
        if not prefix or not environment or not application_id:
            raise ValueError("topic prefix, environment and application_id must not be empty")
        return f"{prefix}.{environment}.{application_id}"

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
        *,
        environment: str = "local",
    ) -> str:
        role = "short_queue" if priority >= QueuePriority.NORMAL else "long_queue"
        override = os.environ.get(f"UR_TOPIC_{role.upper()}")
        if override:
            return override
        root = TopicNames.application_root(prefix, environment, application_id)
        return f"{root}.runs.{role}.v1"
