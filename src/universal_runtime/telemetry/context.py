from __future__ import annotations

from contextlib import contextmanager
from typing import Any

import structlog


@contextmanager
def bind_context(**kwargs: Any) -> Any:
    saved = {
        key: structlog.contextvars.get_contextvars().get(key)
        for key in kwargs
    }
    structlog.contextvars.bind_contextvars(**kwargs)
    try:
        yield
    finally:
        structlog.contextvars.clear_contextvars()
        if saved:
            structlog.contextvars.bind_contextvars(
                **{k: v for k, v in saved.items() if v is not None}
            )


def set_run_context(
    run_id: str,
    *,
    thread_id: str | None = None,
    assistant_id: str | None = None,
    attempt_id: str | None = None,
) -> None:
    structlog.contextvars.bind_contextvars(
        run_id=run_id,
        thread_id=thread_id,
        assistant_id=assistant_id,
        attempt_id=attempt_id,
    )
