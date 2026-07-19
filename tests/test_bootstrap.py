import json
from pathlib import Path

from universal_runtime.bootstrap.cli import main

ROOT = Path(__file__).resolve().parents[1]


def test_launcher_info(capsys) -> None:
    assert main(["info"]) == 0
    assert json.loads(capsys.readouterr().out)["profile"] == "bootstrap"


def test_launcher_validates_example_config(capsys) -> None:
    assert main(["validate-config", str(ROOT / "runtime.example.yaml")]) == 0
    assert "config: valid" in capsys.readouterr().out


def test_launcher_returns_error_for_invalid_config(capsys) -> None:
    path = ROOT / ".invalid-runtime-test.yaml"
    path.write_text("apiVersion: runtime.ai/v1alpha1\n", encoding="utf-8")
    assert main(["validate-config", str(path)]) == 1
    assert "required property" in capsys.readouterr().out
    path.unlink(missing_ok=True)
