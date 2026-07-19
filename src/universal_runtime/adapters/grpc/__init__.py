from __future__ import annotations

from typing import Any

from .worker import BoundedWorker, ExecutionServicer, WorkerConfig, WorkerControlServicer

__all__ = [
    "BoundedWorker",
    "ExecutionServicer",
    "WorkerConfig",
    "WorkerControlServicer",
    "WorkerServer",
]


def __getattr__(name: str) -> Any:
    if name == "WorkerServer":
        from .server import WorkerServer

        return WorkerServer
    raise AttributeError(name)
