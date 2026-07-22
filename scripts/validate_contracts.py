from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[1]

OPENAPI_PATH = ROOT / "contracts/openapi/universal-runtime-phase1.yaml"
SCOPE_PATH = ROOT / "contracts/openapi/COMPATIBILITY_SCOPE.json"

EXPECTED_OPERATION_IDS = {
    "health": "health",
    "server_info": "info",
    "assistant_create": "createAssistant",
    "assistant_get": "getAssistant",
    "assistant_search": "searchAssistants",
    "assistant_schemas": "getAssistantSchemas",
    "thread_create": "createThread",
    "thread_get": "getThread",
    "thread_state_get": "getThreadState",
    "thread_state_update": "updateThreadState",
    "thread_history": "getThreadHistory",
    "run_create": "createRun",
    "run_stream": "streamRun",
    "stateless_run_stream": "streamStatelessRun",
    "stateless_run_wait": "waitStatelessRun",
    "run_get": "getRun",
    "run_cancel": "cancelRun",
    "agent_card": "getAgentCard",
    "config_get": "getApplicationConfig",
    "config_revision_create": "createApplicationConfigRevision",
    "config_validate": "validateApplicationConfig",
    "config_activate": "activateApplicationConfigRevision",
}


def main() -> None:
    schema_path = ROOT / "contracts/config/runtime-application.schema.json"
    event_path = ROOT / "contracts/events/runtime-event-v1.schema.json"
    runtime_example_path = ROOT / "deployment" / "runtime.example.yaml"
    openapi_path = OPENAPI_PATH
    scope_path = SCOPE_PATH

    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    event_schema = json.loads(event_path.read_text(encoding="utf-8"))
    runtime_example = yaml.safe_load(runtime_example_path.read_text(encoding="utf-8"))
    openapi = yaml.safe_load(openapi_path.read_text(encoding="utf-8"))
    scope = json.loads(scope_path.read_text(encoding="utf-8"))

    Draft202012Validator.check_schema(schema)
    Draft202012Validator.check_schema(event_schema)
    Draft202012Validator(schema).validate(runtime_example)

    if openapi.get("openapi") != "3.1.0":
        raise SystemExit("OpenAPI document must be 3.1.0")
    if not openapi.get("paths"):
        raise SystemExit("OpenAPI paths are empty")
    operation_ids = {
        operation.get("operationId")
        for path_item in openapi["paths"].values()
        for operation in path_item.values()
        if isinstance(operation, dict) and "operationId" in operation
    }
    missing_operations = set(EXPECTED_OPERATION_IDS.values()).difference(operation_ids)
    if missing_operations:
        raise SystemExit(f"OpenAPI operation IDs missing: {sorted(missing_operations)}")
    required_scope_fields = {"contracted", "implemented", "verified", "todo"}
    if not required_scope_fields.issubset(set(scope)):
        raise SystemExit(
            "compatibility scope must contain contracted, implemented, verified and todo"
        )
    if not set(scope.get("verified", [])).issubset(set(scope.get("implemented", []))):
        raise SystemExit("verified compatibility entries must be implemented")
    if set(scope.get("implemented", [])) != set(EXPECTED_OPERATION_IDS):
        raise SystemExit("compatibility scope does not match the OpenAPI implementation set")
    todo_ids = [item.get("id") for item in scope.get("todo", [])]
    if len(todo_ids) != len(set(todo_ids)) or not all(todo_ids):
        raise SystemExit("compatibility TODO entries need unique IDs")

    output = ROOT / ".contract-validation"
    output.mkdir(exist_ok=True)
    descriptor = output / "runtime.pb"
    command = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        "-I",
        str(ROOT / "contracts/proto"),
        f"--descriptor_set_out={descriptor}",
        "--include_imports",
        str(ROOT / "contracts/proto/runtime/v1/execution.proto"),
        str(ROOT / "contracts/proto/runtime/v1/worker.proto"),
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=False)  # noqa: S603
    descriptor.unlink(missing_ok=True)
    if result.returncode != 0:
        raise SystemExit(f"protobuf compilation failed:\n{result.stderr}")
    print("contracts: valid")


if __name__ == "__main__":
    main()
