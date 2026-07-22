import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_architecture_guard_passes() -> None:
    result = subprocess.run(  # noqa: S603
        [sys.executable, "scripts/check_architecture.py"], cwd=ROOT, capture_output=True, text=True
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_contract_validator_passes() -> None:
    result = subprocess.run(  # noqa: S603
        [sys.executable, "scripts/validate_contracts.py"], cwd=ROOT, capture_output=True, text=True
    )
    assert result.returncode == 0, result.stdout + result.stderr
