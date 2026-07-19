from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_PATH = PROJECT_ROOT / "contracts" / "config" / "runtime-application.schema.json"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="runtime-launcher")
    parser.add_argument("--version", action="version", version="universal-runtime 0.1.0")
    commands = parser.add_subparsers(dest="command")
    commands.add_parser("info", help="print runtime bootstrap information")
    validate = commands.add_parser("validate-config", help="validate a runtime YAML file")
    validate.add_argument("path", type=Path)
    return parser


def _validate_config(path: Path) -> int:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    errors = sorted(Draft202012Validator(schema).iter_errors(document), key=str)
    if errors:
        for error in errors:
            print(f"{error.json_path}: {error.message}")
        return 1
    print(f"config: valid ({path})")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "validate-config":
        return _validate_config(args.path)
    if args.command == "info":
        print(json.dumps({"version": "0.1.0", "profile": "bootstrap"}))
        return 0
    _parser().print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
