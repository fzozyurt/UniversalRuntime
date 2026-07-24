from __future__ import annotations

from universal_runtime.domain.execution import RunCommand


class PartitionKey:
    """Preserve per-thread order while distributing stateless runs independently."""

    @staticmethod
    def for_command(command: RunCommand) -> str:
        identity = command.identity
        affinity = identity.thread_id or identity.run_id
        return f"{identity.application_id}:{affinity}"
