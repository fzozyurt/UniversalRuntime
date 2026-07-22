from __future__ import annotations

from dataclasses import dataclass

from universal_runtime.domain.capabilities.enums import RuntimeProfile, SessionAffinity, StreamMode


@dataclass(frozen=True, slots=True)
class IdentityCapabilities:
    accepts_thread_id: bool = True
    accepts_run_id: bool = True


@dataclass(frozen=True, slots=True)
class AdapterCapabilities:
    streaming: bool = True
    cancellation: bool = True
    history: bool = False
    checkpoint: bool = False
    state_management: bool = False
    interrupt: bool = False
    resume: bool = False
    fork: bool = False
    custom_http: bool = False
    a2a: bool = False
    subagents: bool = False


@dataclass(frozen=True, slots=True)
class AdapterManifest:
    adapter_id: str
    adapter_version: str
    profiles: frozenset[RuntimeProfile | str]
    stream_modes: frozenset[StreamMode | str]
    capabilities: AdapterCapabilities
    identity: IdentityCapabilities = IdentityCapabilities()
    session_affinity: SessionAffinity = SessionAffinity.NONE

    @property
    def supported_profiles(self) -> tuple[str, ...]:
        return tuple(sorted(str(x) for x in self.profiles))

    @property
    def supported_stream_modes(self) -> tuple[str, ...]:
        return tuple(sorted(str(x) for x in self.stream_modes))
