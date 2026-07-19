from __future__ import annotations

from enum import StrEnum


class LangGraphErrorCode(StrEnum):
    CAPABILITY_NOT_SUPPORTED = "CAPABILITY_NOT_SUPPORTED"
    INVALID_GRAPH = "INVALID_GRAPH"
    INVALID_PERSISTENCE = "INVALID_PERSISTENCE"
    LOAD_FAILED = "LOAD_FAILED"


class LangGraphAdapterError(RuntimeError):
    def __init__(self, code: LangGraphErrorCode, message: str) -> None:
        super().__init__(message)
        self.code = code
