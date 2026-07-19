from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime

from universal_runtime.domain.execution.errors import RunError
from universal_runtime.domain.execution.statuses import RunStatus, ThreadStatus
from universal_runtime.domain.identity import ExecutionIdentity, RunId, ThreadId
from universal_runtime.domain.primitives.json_types import JsonObject, JsonValue


@dataclass(frozen=True, slots=True)
class Thread:
    thread_id: ThreadId
    status: ThreadStatus = ThreadStatus.IDLE
    metadata: JsonObject = field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None

    def _transition(self, status: ThreadStatus, now: datetime) -> Thread:
        return Thread(self.thread_id, status, deepcopy(self.metadata), self.created_at, now)

    def mark_busy(self, now: datetime) -> Thread:
        return self._transition(ThreadStatus.BUSY, now)

    def mark_idle(self, now: datetime) -> Thread:
        return self._transition(ThreadStatus.IDLE, now)

    def mark_interrupted(self, now: datetime) -> Thread:
        return self._transition(ThreadStatus.INTERRUPTED, now)

    def mark_error(self, now: datetime) -> Thread:
        return self._transition(ThreadStatus.ERROR, now)


@dataclass(frozen=True, slots=True)
class Run:
    identity: ExecutionIdentity
    status: RunStatus = RunStatus.PENDING
    metadata: JsonObject = field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None
    result: JsonValue = None
    error: RunError | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", deepcopy(self.metadata))

    def _change(
        self,
        status: RunStatus,
        now: datetime,
        *,
        result: JsonValue = None,
        error: RunError | None = None,
    ) -> Run:
        if (
            self.status
            in {RunStatus.SUCCESS, RunStatus.ERROR, RunStatus.TIMEOUT, RunStatus.CANCELLED}
            and status != self.status
        ):
            raise ValueError(f"terminal run cannot transition from {self.status} to {status}")
        return Run(self.identity, status, self.metadata, self.created_at, now, result, error)

    def mark_running(self, now: datetime) -> Run:
        return self._change(RunStatus.RUNNING, now)

    def complete(self, result: JsonValue, now: datetime) -> Run:
        return self._change(RunStatus.SUCCESS, now, result=result)

    def fail(self, error: RunError, now: datetime) -> Run:
        return self._change(RunStatus.ERROR, now, error=error)

    def cancel(self, now: datetime) -> Run:
        if self.status == RunStatus.CANCELLED:
            return self
        return self._change(RunStatus.CANCELLED, now)

    @property
    def run_id(self) -> RunId:
        return self.identity.run_id

    @property
    def thread_id(self) -> ThreadId | None:
        return self.identity.thread_id
