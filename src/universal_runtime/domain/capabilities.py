from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

SessionAffinity = Literal["none", "preferred", "required"]


@dataclass(frozen=True, slots=True)
class AdapterCapabilities:
    streaming: bool = True
    cancellation: bool = True
    custom_thread_id: bool = True
    custom_run_id: bool = True
    history: bool = False
    checkpoint: bool = False
    state_management: bool = False
    interrupt: bool = False
    resume: bool = False
    fork: bool = False
    custom_http: bool = False
    a2a: bool = False
    subagents: bool = False
    session_affinity: SessionAffinity = "none"
