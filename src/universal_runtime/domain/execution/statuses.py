from enum import StrEnum


class RunStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    INTERRUPTED = "interrupted"
    SUCCESS = "success"
    ERROR = "error"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


class ThreadStatus(StrEnum):
    IDLE = "idle"
    BUSY = "busy"
    INTERRUPTED = "interrupted"
    ERROR = "error"
