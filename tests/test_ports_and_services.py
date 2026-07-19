from services.dispatcher.main import main as dispatcher_main
from services.event_projector.main import main as projector_main
from services.gateway.main import main as gateway_main
from services.worker.main import main as worker_main

from universal_runtime.domain.capabilities import AdapterCapabilities
from universal_runtime.domain.execution import ExecutionRequest
from universal_runtime.ports.configuration import ApplicationConfigRepository, ConfigRevision
from universal_runtime.ports.queue import RunCommandQueue
from universal_runtime.ports.runtime_adapter import AdapterManifest, RuntimeAdapter


def test_ports_expose_contract_types() -> None:
    capabilities = AdapterCapabilities()
    revision = ConfigRevision("application", 1, {}, "hash", True)
    manifest = AdapterManifest(
        adapter_id="bootstrap",
        adapter_version="0.1",
        profiles=frozenset({"local"}),
        stream_modes=frozenset({"values", "updates", "messages"}),
        capabilities=capabilities,
    )
    assert revision.active is True
    assert manifest.adapter_id == "bootstrap"
    assert manifest.supported_profiles == ("local",)
    assert manifest.supported_stream_modes == ("messages", "updates", "values")
    assert ApplicationConfigRepository and RunCommandQueue and RuntimeAdapter and ExecutionRequest


def test_service_composition_roots_are_callable() -> None:
    assert gateway_main() == dispatcher_main() == worker_main() == projector_main() == 0
