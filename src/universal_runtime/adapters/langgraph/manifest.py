from universal_runtime.domain.capabilities import (
    AdapterCapabilities,
    AdapterManifest,
    IdentityCapabilities,
    RuntimeProfile,
    SessionAffinity,
    StreamMode,
)


def langgraph_manifest() -> AdapterManifest:
    return AdapterManifest(
        adapter_id="langgraph",
        adapter_version="1",
        profiles=frozenset(RuntimeProfile),
        stream_modes=frozenset(StreamMode),
        capabilities=AdapterCapabilities(
            streaming=True,
            cancellation=True,
            checkpoint=True,
            state_management=True,
            history=True,
            interrupt=True,
            resume=True,
            subagents=True,
        ),
        identity=IdentityCapabilities(),
        session_affinity=SessionAffinity.PREFERRED,
    )
