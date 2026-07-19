from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator

from universal_runtime.adapters.fastapi.detector import detect_asgi_application
from universal_runtime.bootstrap.local import create_local_runtime

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_PATH = PROJECT_ROOT / "contracts" / "config" / "runtime-application.schema.json"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="runtime-launcher")
    parser.add_argument("--version", action="version", version="universal-runtime 0.1.0")
    commands = parser.add_subparsers(dest="command")
    commands.add_parser("info", help="print runtime bootstrap information")
    commands.add_parser("standalone", help="create and shut down the local runtime composition")
    inspect = commands.add_parser("inspect", help="discover an application HTTP surface")
    inspect.add_argument("path", type=Path)
    inspect.add_argument("--entrypoint")
    inspect.add_argument("--isolated-import", action="store_true")
    migrate = commands.add_parser("migrate", help="run application migrations")
    migrate.add_argument("--config", required=True)
    migrate.add_argument("--application-id", required=True)
    migrate.add_argument("--environment", default="local")
    api = commands.add_parser("api", help="start the application API composition")
    api.add_argument("--entrypoint", required=True)
    api.add_argument("--root-path", default="")
    api.add_argument("--host", default="127.0.0.1")
    api.add_argument("--port", type=int, default=8000)
    commands.add_parser("worker", help="start the runtime worker composition")
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
    if args.command == "standalone":
        import asyncio

        async def run() -> None:
            runtime = create_local_runtime()
            await runtime.shutdown()

        asyncio.run(run())
        print(json.dumps({"version": "0.1.0", "profile": "standalone"}))
        return 0
    if args.command == "inspect":
        try:
            descriptor = detect_asgi_application(
                args.path, explicit_entrypoint=args.entrypoint, isolated_import=args.isolated_import
            )
        except Exception as exc:
            print(str(exc))
            return 1
        print(json.dumps(descriptor.to_json(), separators=(",", ":")))
        return 0
    if args.command == "api":
        import uvicorn

        from universal_runtime.adapters.fastapi.app_server import load_application

        uvicorn.run(
            load_application(args.entrypoint, root_path=args.root_path),
            host=args.host,
            port=args.port,
        )
        return 0
    if args.command == "worker":
        print(json.dumps({"profile": "worker", "status": "composition-required"}))
        return 0
    if args.command == "migrate":
        print(
            json.dumps(
                {
                    "profile": "migrate",
                    "config": args.config,
                    "application_id": args.application_id,
                    "environment": args.environment,
                }
            )
        )
        return 0
    _parser().print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
