from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.store.memory import InMemoryStore

from universal_runtime.adapters.langgraph.errors import LangGraphAdapterError, LangGraphErrorCode


@dataclass(frozen=True, slots=True)
class PersistenceProviders:
    checkpointer: Any | None
    store: Any | None


def local_persistence(mode: str = "platform-managed") -> PersistenceProviders:
    if mode == "disabled":
        return PersistenceProviders(None, None)
    return PersistenceProviders(InMemorySaver(), InMemoryStore())


def validate_persistence(mode: str, *, has_checkpointer: bool) -> None:
    if mode == "platform-managed" and has_checkpointer:
        raise LangGraphAdapterError(
            LangGraphErrorCode.INVALID_PERSISTENCE,
            "application supplied a checkpointer in platform-managed mode",
        )
