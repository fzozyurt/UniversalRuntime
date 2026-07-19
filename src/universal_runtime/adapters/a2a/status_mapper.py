from __future__ import annotations

from typing import Any

from universal_runtime.domain.events import RuntimeEventType


def task_state(event_type: RuntimeEventType | str) -> Any:
    from a2a.types import TaskState

    value = str(event_type)
    return {
        "run.queued": TaskState.TASK_STATE_SUBMITTED,
        "run.started": TaskState.TASK_STATE_WORKING,
        "run.interrupted": TaskState.TASK_STATE_INPUT_REQUIRED,
        "run.completed": TaskState.TASK_STATE_COMPLETED,
        "run.cancelled": TaskState.TASK_STATE_CANCELED,
        "run.failed": TaskState.TASK_STATE_FAILED,
        "run.timeout": TaskState.TASK_STATE_FAILED,
    }.get(value)
