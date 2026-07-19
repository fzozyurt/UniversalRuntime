from __future__ import annotations

import json
import sys
from pathlib import Path

from fastapi.testclient import TestClient

from universal_runtime.bootstrap.cli import main
from universal_runtime.services.gateway.app import create_app


def test_graph_inspection_command_emits_descriptor(capsys, monkeypatch) -> None:
    example_src = Path(__file__).parents[1] / "examples" / "phase1-agent" / "src"
    monkeypatch.setenv("PYTHONPATH", str(example_src))
    sys.path.insert(0, str(example_src))
    assert main(["inspect-graph", "--entrypoint", "phase1_agent.graph:graph"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["object_kind"] == "compiled"


def test_gateway_readiness_and_instance_identity(monkeypatch) -> None:
    monkeypatch.setenv("UR_INSTANCE_ID", "gateway-test")
    response = TestClient(create_app()).get("/ready")
    assert response.status_code == 200
    assert response.json() == {"ready": True}
    assert response.headers["x-runtime-instance-id"] == "gateway-test"
