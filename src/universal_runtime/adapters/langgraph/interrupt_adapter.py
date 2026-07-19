from __future__ import annotations

from typing import Any

from langgraph.types import Command


def resume_command(value: Any) -> Command:
    return Command(resume=value)
