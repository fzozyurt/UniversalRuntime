from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ExecutionTarget:
    """Immutable executable selection pinned when a run is created.

    Application, revision and deployment identity remain authoritative in
    ``ExecutionIdentity``. This value object only selects the graph executable
    and the immutable assistant configuration version used by the worker.
    """

    graph_id: str
    assistant_version: int = 1

    def __post_init__(self) -> None:
        if not self.graph_id.strip():
            raise ValueError("execution target graph_id must not be empty")
        if self.assistant_version < 1:
            raise ValueError("assistant_version must be positive")
