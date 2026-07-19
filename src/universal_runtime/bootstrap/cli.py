from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence
from dataclasses import asdict
from pathlib import Path
from typing import Any, cast

import yaml
from jsonschema import Draft202012Validator

from universal_runtime.adapters.fastapi.detector import detect_asgi_application
from universal_runtime.bootstrap.local import create_local_runtime

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_PATH = PROJECT_ROOT / "contracts" / "config" / "runtime-application.schema.json"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="universal-runtime")
    parser.add_argument("--version", action="version", version="universal-runtime 0.1.0")
    commands = parser.add_subparsers(dest="command")
    commands.add_parser("info", help="print runtime bootstrap information")
    commands.add_parser("validate", help="validate UR_CONFIG_PATH")
    commands.add_parser("standalone", help="create and shut down the local runtime composition")
    inspect = commands.add_parser("inspect", help="discover an application HTTP surface")
    inspect.add_argument("path", type=Path)
    inspect.add_argument("--entrypoint")
    inspect.add_argument("--isolated-import", action="store_true")
    graph_inspect = commands.add_parser(
        "inspect-graph", help="inspect an application graph entrypoint"
    )
    graph_inspect.add_argument("--entrypoint", required=True)
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
    commands.add_parser("gateway", help="start the Gateway HTTP process")
    commands.add_parser("dispatcher", help="start the dispatcher process")
    commands.add_parser("projector", help="start the event projector process")
    validate = commands.add_parser("validate-config", help="validate a runtime YAML file")
    validate.add_argument("path", type=Path)
    return parser


def _validate_config(path: Path) -> int:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    schema_path = Path(os.environ.get("UR_CONTRACT_SCHEMA_PATH", str(SCHEMA_PATH)))
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    errors = sorted(Draft202012Validator(schema).iter_errors(document), key=str)
    if errors:
        for error in errors:
            print(f"{error.json_path}: {error.message}")
        return 1
    print(f"config: valid ({path})")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    effective_argv = list(argv) if argv is not None else None
    if not effective_argv and os.environ.get("UR_MODE"):
        mode = os.environ["UR_MODE"]
        command_argv = sys.argv[1:]
        effective_argv = command_argv if command_argv[:1] == [mode] else [mode, *command_argv]
    args = _parser().parse_args(effective_argv)
    if args.command == "validate-config":
        return _validate_config(args.path)
    if args.command == "info":
        print(json.dumps({"version": "0.1.0", "profile": "bootstrap"}))
        return 0
    if args.command == "validate":
        config_path = Path(os.environ.get("UR_CONFIG_PATH", "runtime.yaml"))
        if not config_path.exists():
            print(f"config: missing ({config_path})")
            return 1
        return _validate_config(config_path)
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
            asgi_descriptor = detect_asgi_application(
                args.path, explicit_entrypoint=args.entrypoint, isolated_import=args.isolated_import
            )
        except Exception as exc:
            print(str(exc))
            return 1
        print(json.dumps(asgi_descriptor.to_json(), separators=(",", ":")))
        return 0
    if args.command == "inspect-graph":
        from importlib import import_module

        from universal_runtime.adapters.langgraph.detector import detect_graph

        module_name, attribute = args.entrypoint.split(":", 1)
        target = getattr(import_module(module_name), attribute)
        graph_descriptor = detect_graph(target, entrypoint=args.entrypoint)
        print(json.dumps(asdict(graph_descriptor), separators=(",", ":"), default=str))
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
    if args.command in {"all", "gateway", "dispatcher", "projector", "worker"}:
        service_main: Any
        if args.command == "all":
            from universal_runtime.services.all.main import main as service_main
        elif args.command == "gateway":
            from universal_runtime.services.gateway.main import main as service_main
        elif args.command == "dispatcher":
            from universal_runtime.services.dispatcher.main import main as service_main
        elif args.command == "projector":
            from universal_runtime.services.event_projector.main import main as service_main
        else:
            from universal_runtime.services.worker.main import main as service_main
        return cast(int, service_main(run_forever=True))
    if args.command == "migrate":
        import asyncio

        from universal_runtime.adapters.postgres.database import create_engine
        from universal_runtime.adapters.postgres.migration import migrate_platform

        database_url = os.environ.get("UR_DATABASE_URL")
        if not database_url:
            print("UR_DATABASE_URL is required for migrations")
            return 2

        async def run_migration() -> None:
            engine = create_engine(database_url)
            try:
                await migrate_platform(
                    engine,
                    application_id=args.application_id,
                    environment=args.environment,
                )
            finally:
                await engine.dispose()

        asyncio.run(run_migration())
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
