from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any

from universal_runtime.domain.identity import ExecutionIdentity


class QueuePriority(IntEnum):
    BATCH = 10
    NORMAL = 50
    INTERACTIVE = 100


@dataclass(frozen=True, slots=True)
class ExecutionRequest:
    identity: ExecutionIdentity
    assistant_id: str
    input: Any = None
    command: Any = None
    config: dict[str, Any] = field(default_factory=dict)
    context: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    stream_modes: tuple[str, ...] = ("values",)
    stream_subgraphs: bool = False
    priority: QueuePriority = QueuePriority.INTERACTIVE
    timeout_seconds: int = 1800
