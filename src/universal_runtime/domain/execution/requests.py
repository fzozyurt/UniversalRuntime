from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime

from universal_runtime.domain.execution.priority import QueuePriority
from universal_runtime.domain.identity import CommandId, ExecutionIdentity, LeaseId
from universal_runtime.domain.primitives.json_types import JsonObject, JsonValue


@dataclass(frozen=True, slots=True)
class ExecutionRequest:
    identity: ExecutionIdentity
    input: JsonValue = None
    command: JsonValue = None
    config: JsonObject = field(default_factory=dict)
    context: JsonObject = field(default_factory=dict)
    metadata: JsonObject = field(default_factory=dict)
    stream_modes: tuple[str, ...] = ("values",)
    stream_subgraphs: bool = False
    priority: QueuePriority = QueuePriority.INTERACTIVE
    timeout_seconds: int = 1800
    checkpoint_namespace: str = ""
    checkpoint_id: str | None = None

    def __post_init__(self) -> None:
        for name in ("config", "context", "metadata"):
            object.__setattr__(self, name, deepcopy(getattr(self, name)))


@dataclass(frozen=True, slots=True)
class RunCommand:
    command_id: CommandId
    identity: ExecutionIdentity
    request: ExecutionRequest
    priority: QueuePriority
    available_at: datetime
    created_at: datetime


@dataclass(frozen=True, slots=True)
class RunCommandReceipt:
    command: RunCommand
    lease_id: LeaseId
    delivery_count: int
    leased_at: datetime
    lease_expires_at: datetime

    @property
    def identity(self) -> ExecutionIdentity:
        return self.command.identity
