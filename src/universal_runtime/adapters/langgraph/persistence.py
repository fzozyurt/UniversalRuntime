from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.store.memory import InMemoryStore

from universal_runtime.adapters.langgraph.errors import LangGraphAdapterError, LangGraphErrorCode
from universal_runtime.domain.capabilities import SessionAffinity


class PersistenceProvider(StrEnum):
    MEMORY = "memory"
    POSTGRES = "postgres"
    DISABLED = "disabled"


@dataclass(frozen=True, slots=True)
class PersistenceProviders:
    checkpointer: Any | None
    store: Any | None
    provider: PersistenceProvider = PersistenceProvider.MEMORY
    session_affinity: SessionAffinity = SessionAffinity.REQUIRED


def local_persistence(mode: str = "platform-managed") -> PersistenceProviders:
    if mode == "disabled":
        return PersistenceProviders(None, None, PersistenceProvider.DISABLED, SessionAffinity.NONE)
    return PersistenceProviders(InMemorySaver(), InMemoryStore())


def postgres_persistence(checkpointer: Any, store: Any | None) -> PersistenceProviders:
    return PersistenceProviders(
        checkpointer, store, PersistenceProvider.POSTGRES, SessionAffinity.NONE
    )


def validate_persistence(mode: str, *, has_checkpointer: bool) -> None:
    if mode not in {"disabled", "platform-managed", "application-managed"}:
        raise LangGraphAdapterError(
            LangGraphErrorCode.INVALID_PERSISTENCE, f"unknown persistence mode: {mode}"
        )

    if mode == "platform-managed" and has_checkpointer:
        raise LangGraphAdapterError(
            LangGraphErrorCode.INVALID_PERSISTENCE,
            "application supplied a checkpointer in platform-managed mode",
        )
