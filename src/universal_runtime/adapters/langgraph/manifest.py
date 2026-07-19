from universal_runtime.domain.capabilities import (
    AdapterCapabilities,
    AdapterManifest,
    IdentityCapabilities,
    RuntimeProfile,
    SessionAffinity,
    StreamMode,
)


def langgraph_manifest(
    *,
    session_affinity: SessionAffinity = SessionAffinity.NONE,
    cancellation: bool = True,
    checkpoint: bool = True,
    history: bool = True,
    state_management: bool = True,
    interrupt: bool = True,
    resume: bool = True,
) -> AdapterManifest:
    return AdapterManifest(
        adapter_id="langgraph",
        adapter_version="1",
        profiles=frozenset(RuntimeProfile),
        stream_modes=frozenset(StreamMode),
        capabilities=AdapterCapabilities(
            streaming=True,
            cancellation=cancellation,
            checkpoint=checkpoint,
            state_management=state_management,
            history=history,
            interrupt=interrupt,
            resume=resume,
            subagents=True,
        ),
        identity=IdentityCapabilities(),
        session_affinity=session_affinity,
    )
