from __future__ import annotations

import asyncio
import importlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from fastapi import Body, FastAPI, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from jsonschema import Draft202012Validator

from universal_runtime.adapters.a2a.server import create_a2a_routes
from universal_runtime.adapters.langgraph import LangGraphAdapter
from universal_runtime.adapters.langgraph.persistence import postgres_persistence
from universal_runtime.adapters.postgres.database import create_engine
from universal_runtime.adapters.postgres.langgraph import managed_langgraph_persistence
from universal_runtime.bootstrap.local import LocalRuntime, create_local_runtime
from universal_runtime.bootstrap.production import create_production_runtime
from universal_runtime.domain.assistants import Assistant
from universal_runtime.domain.capabilities import AdapterManifest
from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.domain.execution import ExecutionRequest, QueuePriority, RunStatus
from universal_runtime.domain.identity import (
    ApplicationId,
    ApplicationScope,
    AssistantId,
    AttemptId,
    CommandId,
    DeploymentId,
    ExecutionIdentity,
    ProjectId,
    RevisionId,
    RunId,
    ThreadId,
    WorkspaceId,
)
from universal_runtime.domain.primitives.json_types import JsonObject, JsonValue
from universal_runtime.ports.runtime_adapter import RuntimeAdapter
from universal_runtime.services.gateway.custom_http_routes import create_custom_http_router
from universal_runtime.telemetry.bootstrap import initialize
from universal_runtime.telemetry.instrumentation import instrument_clients, instrument_fastapi
from universal_runtime.transport.http.dto import (
    AssistantCreate,
    NativeErrorResponse,
    NativeMeta,
    NativeResponse,
    RunCreate,
    RuntimeErrorBody,
    ThreadCreate,
)

SCHEMA_PATH = (
    Path(__file__).resolve().parents[4] / "contracts" / "config" / "runtime-application.schema.json"
)


def create_app(
    runtime: LocalRuntime | None = None,
    *,
    runtime_adapter: RuntimeAdapter | None = None,
    custom_http_target: str | None = None,
    a2a_assistant: Assistant | None = None,
    a2a_manifest: AdapterManifest | None = None,
    a2a_public_url: str = "http://localhost:8080",
) -> FastAPI:
    state = runtime or (
        create_production_runtime()
        if os.environ.get("UR_PROFILE", "local") == "production"
        else create_local_runtime()
    )
    if runtime_adapter is not None:
        state.adapters.register(runtime_adapter)
    schema = _load_schema()
    app = FastAPI(title="UniversalRuntime Gateway", version="0.1.0")
    app.state.telemetry = initialize(component="gateway")
    instrument_clients()
    instrument_fastapi(app)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.state.runtime = state
    app.state.worker_registry = {}

    @app.on_event("startup")
    async def start_local_execution() -> None:
        await _auto_register_application(state, app)
        await state.start()

    @app.on_event("shutdown")
    async def stop_local_execution() -> None:
        await state.shutdown()
        context = getattr(app.state, "langgraph_context", None)
        if context is not None:
            await context.__aexit__(None, None, None)
        migration_engine = getattr(app.state, "langgraph_migration_engine", None)
        if migration_engine is not None:
            await migration_engine.dispose()

    if custom_http_target is not None:
        app.include_router(create_custom_http_router(custom_http_target))
    if a2a_assistant is not None and a2a_manifest is not None:
        for route in create_a2a_routes(
            runtime=state,
            assistant=a2a_assistant,
            manifest=a2a_manifest,
            public_url=a2a_public_url,
        ):
            app.routes.append(route)

    @app.middleware("http")
    async def request_id(request: Request, call_next: Any) -> Any:
        request.state.request_id = request.headers.get("x-request-id") or str(CommandId.new())
        response = await call_next(request)
        response.headers["x-runtime-instance-id"] = os.environ.get("UR_INSTANCE_ID", "local")
        return response

    @app.exception_handler(RuntimeFailure)
    async def runtime_failure(request: Request, exc: RuntimeFailure) -> JSONResponse:
        status = _status_for(exc.code)
        if request.url.path.startswith("/api/v1/"):
            return JSONResponse(
                status_code=status,
                content=NativeErrorResponse(
                    error=RuntimeErrorBody(
                        code=str(exc.code),
                        message=exc.message,
                        retryable=exc.retryable,
                        request_id=request.state.request_id,
                        details=exc.details,
                    )
                ).model_dump(mode="json"),
            )
        return JSONResponse(status_code=status, content={"detail": exc.message})

    @app.exception_handler(RequestValidationError)
    async def request_validation(request: Request, exc: RequestValidationError) -> JSONResponse:
        if request.url.path.startswith("/api/v1/"):
            return JSONResponse(
                status_code=422,
                content=NativeErrorResponse(
                    error=RuntimeErrorBody(
                        code="VALIDATION_ERROR",
                        message="request validation failed",
                        retryable=False,
                        request_id=request.state.request_id,
                        details={"errors": cast(JsonObject, jsonable_encoder(exc.errors()))},
                    )
                ).model_dump(mode="json"),
            )
        return JSONResponse(status_code=422, content={"detail": jsonable_encoder(exc.errors())})

    @app.get("/ok")
    async def health() -> dict[str, bool]:
        return {"ok": True}

    @app.get("/ready")
    async def readiness() -> dict[str, bool]:
        return {"ready": True}

    @app.get("/info")
    async def info() -> dict[str, Any]:
        return {
            "version": "0.1.0",
            "adapters": [
                adapter.manifest.__dict__ if hasattr(adapter.manifest, "__dict__") else {}
                for adapter in state.adapters.all()
            ],
        }

    @app.post("/internal/workers/register")
    async def register_worker(payload: JsonObject = Body(...)) -> dict[str, Any]:
        worker_id = str(payload.get("worker_id", ""))
        target = str(payload.get("target", ""))
        if not worker_id or not target:
            raise RuntimeFailure(
                ErrorCode.INVALID_EXECUTION_INPUT,
                "worker_id and target are required",
            )
        app.state.worker_registry[worker_id] = {
            **payload,
            "registered_at": datetime.now(UTC).isoformat(),
        }
        return {"registered": True, "worker_id": worker_id, "target": target}

    @app.get("/internal/workers")
    async def list_workers() -> list[JsonObject]:
        return list(app.state.worker_registry.values())

    @app.post("/api/v1/applications/{application_id}/config/validate")
    async def validate_config(
        application_id: str, request: Request, payload: JsonObject = Body(...)
    ) -> NativeResponse:
        del application_id
        errors = _validation_errors(schema, payload)
        return _native(
            {"valid": not errors, "errors": errors},
            request,
        )

    @app.get("/api/v1/applications/{application_id}/config")
    async def get_config(application_id: str, request: Request) -> NativeResponse:
        revision = await state.config.get_active(application_id)
        return _native(_revision_payload(revision), request)

    @app.put("/api/v1/applications/{application_id}/config", status_code=201)
    async def create_config_revision(
        application_id: str, request: Request, payload: JsonObject = Body(...)
    ) -> NativeResponse:
        errors = _validation_errors(schema, payload)
        if errors:
            raise RuntimeFailure(
                ErrorCode.INVALID_EXECUTION_INPUT,
                "application config is invalid",
                details={"errors": errors},
            )
        revision = await state.config.create_revision(application_id, payload)
        return _native(_revision_payload(revision), request)

    @app.post("/api/v1/applications/{application_id}/config/{revision}/activate")
    async def activate_config(
        application_id: str, revision: int, request: Request
    ) -> NativeResponse:
        activated = await state.config.activate(application_id, revision)
        return _native(_revision_payload(activated), request)

    @app.post("/assistants")
    async def create_assistant(payload: AssistantCreate) -> dict[str, Any]:
        if payload.assistant_id and payload.if_exists == "do_nothing":
            try:
                return _assistant_payload(await state.assistants.get(payload.assistant_id))
            except RuntimeFailure as exc:
                if exc.code is not ErrorCode.RESOURCE_NOT_FOUND:
                    raise
        graph_id = payload.graph_id or "default"
        assistant_id = (
            AssistantId.parse(payload.assistant_id) if payload.assistant_id else AssistantId.new()
        )
        from universal_runtime.domain.assistants import Assistant

        assistant = Assistant(
            assistant_id=assistant_id,
            graph_id=graph_id,
            name=payload.name,
            config=payload.config,
            context=payload.context,
            metadata=payload.metadata,
        )
        created = await state.assistants.create(assistant)
        return _assistant_payload(created)

    @app.post("/assistants/count")
    async def count_assistants(payload: JsonObject | None = Body(default=None)) -> int:
        criteria = payload or {}
        return await state.assistants.count(graph_id=criteria.get("graph_id"))

    @app.post("/assistants/{assistant_id}/versions")
    async def assistant_versions(assistant_id: str, request: Request) -> list[Any]:
        raw = await request.body()
        try:
            decoded = json.loads(raw) if raw else {}
            if isinstance(decoded, str):
                decoded = json.loads(decoded)
        except json.JSONDecodeError as exc:
            raise RuntimeFailure(ErrorCode.INVALID_EXECUTION_INPUT, "invalid JSON body") from exc
        if not isinstance(decoded, dict):
            raise RuntimeFailure(
                ErrorCode.INVALID_EXECUTION_INPUT, "versions body must be an object"
            )
        request_payload = cast(JsonObject, decoded)
        limit = max(0, min(int(request_payload.get("limit", 10)), 1000))
        offset = max(0, int(request_payload.get("offset", 0)))
        versions = await state.assistants.versions(assistant_id)
        return [_assistant_payload(item) for item in versions[offset : offset + limit]]

    @app.post("/assistants/{assistant_id}/latest")
    async def set_latest_assistant(
        assistant_id: str, payload: JsonObject = Body(...)
    ) -> dict[str, Any]:
        version = int(payload.get("version", 0))
        if version < 1:
            raise RuntimeFailure(ErrorCode.INVALID_EXECUTION_INPUT, "version must be positive")
        return _assistant_payload(await state.assistants.set_latest(assistant_id, version))

    @app.get("/assistants/search")
    async def search_assistants_get(
        graph_id: str | None = None,
        name: str | None = None,
        metadata: str | None = None,
        limit: int = Query(10, ge=0, le=1000),
        offset: int = Query(0, ge=0),
    ) -> list[Any]:
        items = await state.assistants.all()
        metadata_filter = json.loads(metadata) if metadata else {}
        filtered = [
            item
            for item in items
            if (graph_id is None or item.graph_id == graph_id)
            and (name is None or item.name == name)
            and all(item.metadata.get(key) == value for key, value in metadata_filter.items())
        ]
        return [_assistant_payload(item) for item in filtered[offset : offset + limit]]

    @app.get("/assistants/{assistant_id}/graph")
    async def assistant_graph(assistant_id: str, xray: bool = False) -> Any:
        await state.assistants.get(assistant_id)
        adapter = _runtime_adapter(state)
        get_graph = getattr(adapter, "get_graph", None)
        if get_graph is None:
            raise RuntimeFailure(
                ErrorCode.CAPABILITY_NOT_SUPPORTED, "graph inspection is not supported"
            )
        return await get_graph(xray=xray)

    @app.get("/assistants/{assistant_id}/subgraphs")
    async def assistant_subgraphs(assistant_id: str, xray: bool = False) -> dict[str, Any]:
        await state.assistants.get(assistant_id)
        adapter = _runtime_adapter(state)
        get_subgraphs = getattr(adapter, "get_subgraphs", None)
        if get_subgraphs is None:
            raise RuntimeFailure(
                ErrorCode.CAPABILITY_NOT_SUPPORTED, "subgraph inspection is not supported"
            )
        return await get_subgraphs(xray=xray)

    @app.get("/assistants/{assistant_id}")
    async def get_assistant(assistant_id: str) -> dict[str, Any]:
        return _assistant_payload(await state.assistants.get(assistant_id))

    @app.patch("/assistants/{assistant_id}")
    async def update_assistant(assistant_id: str, payload: AssistantCreate) -> dict[str, Any]:
        current = await state.assistants.get(assistant_id)
        updated = Assistant(
            assistant_id=current.assistant_id,
            graph_id=payload.graph_id or current.graph_id,
            version=current.version + 1,
            name=payload.name if payload.name is not None else current.name,
            config=payload.config if payload.config else current.config,
            context=payload.context if payload.context else current.context,
            metadata=payload.metadata if payload.metadata else current.metadata,
        )
        return _assistant_payload(await state.assistants.update(assistant_id, updated))

    @app.delete("/assistants/{assistant_id}", status_code=204)
    async def delete_assistant(assistant_id: str, delete_threads: bool = False) -> None:
        await state.assistants.delete(assistant_id, delete_threads=delete_threads)

    @app.post("/assistants/search")
    async def search_assistants(request: Request) -> list[Any]:
        raw = await request.body()
        try:
            decoded = json.loads(raw) if raw else {}
            if isinstance(decoded, str):
                decoded = json.loads(decoded)
        except json.JSONDecodeError as exc:
            raise RuntimeFailure(ErrorCode.INVALID_EXECUTION_INPUT, "invalid JSON body") from exc
        if not isinstance(decoded, dict):
            raise RuntimeFailure(ErrorCode.INVALID_EXECUTION_INPUT, "search body must be an object")
        items = await state.assistants.all()
        criteria = cast(JsonObject, decoded)
        graph_id = criteria.get("graph_id")
        name = criteria.get("name")
        metadata = criteria.get("metadata")
        return [
            _assistant_payload(item)
            for item in items
            if (graph_id is None or item.graph_id == graph_id)
            and (name is None or item.name == name)
            and all(item.metadata.get(key) == value for key, value in (metadata or {}).items())
        ]

    @app.get("/assistants/{assistant_id}/schemas")
    async def assistant_schemas(assistant_id: str) -> dict[str, Any]:
        assistant = await state.assistants.get(assistant_id)
        descriptor = await _runtime_adapter(state).inspect()
        return {
            "graph_id": assistant.graph_id,
            "input_schema": descriptor.input_schema,
            "output_schema": descriptor.output_schema,
            "state_schema": descriptor.state_schema,
            "config_schema": descriptor.config_schema,
            "context_schema": descriptor.context_schema,
        }

    @app.post("/threads")
    async def create_thread(payload: ThreadCreate | None = None) -> dict[str, Any]:
        body = payload or ThreadCreate()
        if body.thread_id and body.if_exists == "do_nothing":
            try:
                return _thread_payload(await state.threads.get(body.thread_id))
            except RuntimeFailure as exc:
                if exc.code is not ErrorCode.RESOURCE_NOT_FOUND:
                    raise
        thread = await state.execution.create_thread(body.thread_id, body.metadata)
        return _thread_payload(thread)

    @app.post("/threads/search")
    async def search_threads(request: Request) -> list[Any]:
        raw = await request.body()
        try:
            decoded = json.loads(raw) if raw else {}
        except json.JSONDecodeError as exc:
            raise RuntimeFailure(ErrorCode.INVALID_EXECUTION_INPUT, "invalid JSON body") from exc
        if not isinstance(decoded, dict):
            raise RuntimeFailure(ErrorCode.INVALID_EXECUTION_INPUT, "search body must be an object")
        payload = cast(JsonObject, decoded)
        items = await state.threads.search(
            metadata=payload.get("metadata") if isinstance(payload.get("metadata"), dict) else None,
            status=str(payload["status"]) if payload.get("status") is not None else None,
            limit=max(0, min(int(payload.get("limit", 10)), 1000)),
            offset=max(0, int(payload.get("offset", 0))),
        )
        return [_thread_payload(item) for item in items]

    @app.post("/threads/count")
    async def count_threads(payload: JsonObject | None = Body(default=None)) -> int:
        criteria = payload or {}
        status = str(criteria["status"]) if criteria.get("status") is not None else None
        metadata = criteria.get("metadata") if isinstance(criteria.get("metadata"), dict) else None
        return await state.threads.count(metadata=metadata, status=status)

    @app.patch("/threads/{thread_id}")
    async def update_thread(
        thread_id: str, request: Request, payload: JsonObject = Body(...)
    ) -> dict[str, Any] | None:
        thread = await state.threads.get(thread_id)
        metadata = payload.get("metadata")
        if not isinstance(metadata, dict):
            raise RuntimeFailure(ErrorCode.INVALID_EXECUTION_INPUT, "metadata must be an object")
        merged = dict(thread.metadata)
        merged.update(metadata)
        updated = await state.threads.update(
            type(thread)(
                thread.thread_id, thread.status, merged, thread.created_at, datetime.now(UTC)
            )
        )
        if request.headers.get("prefer") == "return=minimal":
            return None
        return _thread_payload(updated)

    @app.put("/threads/{thread_id}")
    async def replace_thread(
        thread_id: str, request: Request, payload: JsonObject = Body(...)
    ) -> dict[str, Any] | None:
        return await update_thread(thread_id, request, payload)

    @app.delete("/threads/{thread_id}", status_code=204)
    async def delete_thread(thread_id: str) -> None:
        await state.threads.delete(thread_id)

    @app.post("/threads/{thread_id}/copy", status_code=204)
    async def copy_thread(thread_id: str) -> None:
        source = await state.threads.get(thread_id)
        await state.execution.create_thread(metadata=dict(source.metadata))

    @app.post("/threads/prune")
    async def prune_threads(payload: JsonObject = Body(...)) -> dict[str, int]:
        ids = payload.get("thread_ids")
        if not isinstance(ids, list):
            raise RuntimeFailure(ErrorCode.INVALID_EXECUTION_INPUT, "thread_ids must be a list")
        count = 0
        for thread_id in ids:
            await state.threads.delete(str(thread_id))
            count += 1
        return {"pruned_count": count}

    @app.get("/threads/{thread_id}/state")
    async def get_thread_state(thread_id: str) -> Any:
        run = await _latest_thread_run(state, thread_id)
        adapter = _runtime_adapter(state)
        return _compat_state(await adapter.get_state(ExecutionRequest(identity=run.identity)))

    @app.get("/threads/{thread_id}/state/{checkpoint_id}")
    async def get_thread_state_checkpoint(thread_id: str, checkpoint_id: str) -> Any:
        del checkpoint_id
        return await get_thread_state(thread_id)

    @app.post("/threads/{thread_id}/state")
    async def update_thread_state(thread_id: str, payload: JsonObject = Body(...)) -> Any:
        run = await _latest_thread_run(state, thread_id)
        adapter = _runtime_adapter(state)
        values = payload.get("values", payload)
        result = await adapter.update_state(ExecutionRequest(identity=run.identity), values)
        return _compat_state(result)

    @app.post("/threads/{thread_id}/state/checkpoint")
    async def update_thread_state_checkpoint(
        thread_id: str, payload: JsonObject = Body(...)
    ) -> Any:
        return await update_thread_state(thread_id, payload)

    @app.get("/threads/{thread_id}/history")
    @app.post("/threads/{thread_id}/history")
    async def get_thread_history(thread_id: str, payload: JsonObject | None = Body(None)) -> Any:
        del payload
        await state.threads.get(thread_id)
        run = await state.runs.latest_for_thread(thread_id)
        if run is None:
            return []
        adapter = _runtime_adapter(state)
        history = await adapter.get_state_history(ExecutionRequest(identity=run.identity))
        return [_compat_state(item) for item in history]

    @app.get("/threads/{thread_id}")
    async def get_thread(thread_id: str) -> dict[str, Any]:
        return _thread_payload(await state.threads.get(thread_id))

    @app.post("/threads/{thread_id}/runs")
    async def create_run(thread_id: str, payload: RunCreate) -> dict[str, Any]:
        run = await _start_run(state, payload, thread_id)
        return _run_payload(run)

    @app.get("/threads/{thread_id}/runs")
    async def list_thread_runs(
        thread_id: str,
        limit: int = Query(10, ge=0, le=1000),
        offset: int = Query(0, ge=0),
        status: str | None = None,
    ) -> list[Any]:
        await state.threads.get(thread_id)
        runs = await state.runs.list_for_thread(
            thread_id, limit=limit, offset=offset, status=status
        )
        return [_run_payload(run) for run in runs]

    @app.post("/threads/{thread_id}/runs/stream")
    async def stream_run(thread_id: str, request: Request, payload: RunCreate) -> StreamingResponse:
        run = await _start_run(state, payload, thread_id)
        cursor = (
            await _event_cursor(state, run.run_id)
            if isinstance(payload.command, dict) and "resume" in payload.command
            else None
        )
        return _sse_response(
            state, run.run_id, payload.stream_mode, request=request, after_sequence=cursor
        )

    @app.get("/threads/{thread_id}/runs/{run_id}/stream")
    async def join_stream(
        thread_id: str,
        run_id: str,
        request: Request,
        stream_mode: str | None = None,
    ) -> StreamingResponse:
        run = await state.runs.get(run_id)
        if run.thread_id is None or str(run.thread_id) != thread_id:
            raise RuntimeFailure(ErrorCode.RESOURCE_NOT_FOUND, "run does not belong to thread")
        return _sse_response(state, run.run_id, stream_mode or "values", request=request)

    @app.get("/threads/{thread_id}/stream")
    async def join_thread_stream(
        thread_id: str, request: Request, stream_mode: str | None = None
    ) -> StreamingResponse:
        run = await _latest_thread_run(state, thread_id)
        return _sse_response(state, run.run_id, stream_mode or "values", request=request)

    @app.post("/runs/stream")
    async def stream_stateless_run(request: Request, payload: RunCreate) -> StreamingResponse:
        run = await _start_run(state, payload, None)
        return _sse_response(state, run.run_id, payload.stream_mode, request=request)

    @app.post("/runs")
    async def create_stateless_run(payload: RunCreate) -> dict[str, Any]:
        return _run_payload(await _start_run(state, payload, None))

    @app.post("/runs/batch")
    async def create_run_batch(payloads: list[RunCreate]) -> list[dict[str, Any]]:
        """Create independent stateless runs while preserving per-thread locking."""
        if not payloads:
            return []
        runs = await asyncio.gather(*(_start_run(state, payload, None) for payload in payloads))
        return [_run_payload(run) for run in runs]

    @app.get("/api/v1/runs/{run_id}/events")
    async def native_run_events(run_id: str, request: Request) -> StreamingResponse:
        await state.runs.get(run_id)
        return _sse_response(state, RunId.parse(run_id), "events", request=request, native=True)

    @app.post("/runs/wait")
    async def wait_stateless_run(payload: RunCreate) -> Any:
        run = await _start_run(state, payload, None)
        return await _wait_for_run(state, run.run_id)

    @app.post("/threads/{thread_id}/runs/wait")
    async def wait_thread_run(thread_id: str, payload: RunCreate) -> Any:
        run = await _start_run(state, payload, thread_id)
        return await _wait_for_run(state, run.run_id)

    @app.post("/runs/cancel", status_code=204)
    async def cancel_many_runs(payload: JsonObject = Body(default_factory=dict)) -> None:
        run_ids = payload.get("run_ids")
        if isinstance(run_ids, list):
            for run_id in run_ids:
                await state.execution.cancel_run(str(run_id))
            return
        status = payload.get("status")
        if status in {"pending", "running", "all"}:
            raise RuntimeFailure(
                ErrorCode.CAPABILITY_NOT_SUPPORTED,
                "bulk cancellation by status requires a run index",
            )

    @app.get("/runs/{run_id}")
    async def get_run(run_id: str) -> dict[str, Any]:
        return _run_payload(await state.runs.get(run_id))

    @app.get("/threads/{thread_id}/runs/{run_id}")
    async def get_thread_run(thread_id: str, run_id: str) -> dict[str, Any]:
        run = await state.runs.get(run_id)
        if run.thread_id is None or str(run.thread_id) != thread_id:
            raise RuntimeFailure(ErrorCode.RESOURCE_NOT_FOUND, "run does not belong to thread")
        return _run_payload(run)

    @app.delete("/threads/{thread_id}/runs/{run_id}", status_code=204)
    async def delete_thread_run(thread_id: str, run_id: str) -> None:
        run = await state.runs.get(run_id)
        if run.thread_id is None or str(run.thread_id) != thread_id:
            raise RuntimeFailure(ErrorCode.RESOURCE_NOT_FOUND, "run does not belong to thread")
        deleter = getattr(state.runs, "delete", None)
        if deleter is None:
            raise RuntimeFailure(
                ErrorCode.CAPABILITY_NOT_SUPPORTED, "run deletion is not supported"
            )
        await deleter(run_id)

    @app.get("/threads/{thread_id}/runs/{run_id}/join")
    async def join_run(thread_id: str, run_id: str) -> Any:
        run = await state.runs.get(run_id)
        if run.thread_id is None or str(run.thread_id) != thread_id:
            raise RuntimeFailure(ErrorCode.RESOURCE_NOT_FOUND, "run does not belong to thread")
        return await _wait_for_run(state, run.run_id)

    @app.post("/runs/{run_id}/cancel", status_code=204)
    async def cancel_run(run_id: str) -> None:
        await state.execution.cancel_run(run_id)

    @app.post("/threads/{thread_id}/runs/{run_id}/cancel", status_code=204)
    async def cancel_thread_run(thread_id: str, run_id: str) -> None:
        run = await state.runs.get(run_id)
        if run.thread_id is None or str(run.thread_id) != thread_id:
            raise RuntimeFailure(ErrorCode.RESOURCE_NOT_FOUND, "run does not belong to thread")
        await state.execution.cancel_run(run_id)

    return app


async def _auto_register_application(state: LocalRuntime, app: FastAPI) -> None:
    """Register every explicitly configured graph in the shared Gateway."""
    configured = os.environ.get("UR_APPLICATION_ENTRYPOINTS") or os.environ.get(
        "UR_APPLICATION_ENTRYPOINT"
    )
    entrypoints = [item.strip() for item in (configured or "").split(",") if item.strip()]
    if not entrypoints:
        return

    persistence_mode = os.environ.get("UR_PERSISTENCE_MODE", "platform-managed")
    providers = None
    if os.environ.get("UR_PROFILE", "local") == "production":
        database_url = os.environ["UR_DATABASE_URL"]
        migration_engine = create_engine(database_url)
        context = managed_langgraph_persistence(
            database_url,
            migration_engine=migration_engine,
            application_id=os.environ.get("UR_APPLICATION_ID", "default"),
            environment=os.environ.get("UR_KAFKA_ENVIRONMENT", "local"),
            workspace_key=os.environ.get("UR_WORKSPACE_KEY", "default"),
            application_key=os.environ.get("UR_APPLICATION_ID", "default"),
        )
        persistence = await context.__aenter__()
        app.state.langgraph_context = context
        app.state.langgraph_migration_engine = migration_engine
        providers = postgres_persistence(persistence.checkpointer, persistence.store)

    for entrypoint in entrypoints:
        module_name, attribute = entrypoint.split(":", 1)
        target = getattr(importlib.import_module(module_name), attribute)
        adapter = LangGraphAdapter(target, persistence_mode=persistence_mode, providers=providers)
        state.adapters.register(cast(RuntimeAdapter, adapter))
        assistant_id = adapter.descriptor.graph_id
        assistant = Assistant(
            assistant_id=AssistantId.parse(assistant_id),
            graph_id=adapter.descriptor.graph_id,
            name=(
                os.environ.get("UR_APPLICATION_NAME")
                if len(entrypoints) == 1
                else adapter.descriptor.graph_id
            ),
            metadata={
                "runtime.auto_registered": True,
                "runtime.entrypoint": entrypoint,
                "created_by": "system",
                "workspace_id": os.environ.get("UR_WORKSPACE_ID", "default"),
            },
        )
        try:
            await state.assistants.create(assistant)
        except RuntimeFailure as exc:
            if exc.code is not ErrorCode.INVALID_EXECUTION_INPUT:
                raise


async def _start_run(state: LocalRuntime, payload: RunCreate, thread_id: str | None) -> Any:
    assistant = await state.assistants.get(payload.assistant_id)
    resolved_thread = ThreadId.parse(thread_id) if thread_id is not None else None
    existing = (
        await state.runs.active_for_thread(str(resolved_thread))
        if resolved_thread is not None
        else None
    )
    is_resume = isinstance(payload.command, dict) and "resume" in payload.command
    identity = (
        existing.identity
        if is_resume and existing is not None and existing.status.value == "interrupted"
        else _identity(assistant.assistant_id, RunId.new(), resolved_thread)
    )
    modes = (
        (payload.stream_mode,)
        if isinstance(payload.stream_mode, str)
        else tuple(payload.stream_mode)
    )
    request = ExecutionRequest(
        identity=identity,
        input=cast(JsonValue, payload.input),
        command=cast(JsonValue, payload.command),
        config=payload.config,
        context=payload.context,
        metadata=payload.metadata,
        stream_modes=modes,
        stream_subgraphs=payload.stream_subgraphs,
        priority={
            "interactive": QueuePriority.INTERACTIVE,
            "normal": QueuePriority.NORMAL,
            "batch": QueuePriority.BATCH,
        }[payload.priority],
    )
    local_adapter = bool(state.adapters.all())
    if is_resume and existing is not None and existing.status.value == "interrupted":
        return await state.execution.resume_run(request)
    return await state.execution.start_run(request, outbox=None if local_adapter else state.outbox)


async def _latest_thread_run(state: LocalRuntime, thread_id: str) -> Any:
    await state.threads.get(thread_id)
    run = await state.runs.latest_for_thread(thread_id)
    if run is None:
        raise RuntimeFailure(ErrorCode.RESOURCE_NOT_FOUND, "thread has no runs")
    return run


async def _wait_for_run(state: LocalRuntime, run_id: RunId) -> dict[str, Any]:
    while True:
        run = await state.runs.get(str(run_id))
        if run.status in {
            RunStatus.SUCCESS,
            RunStatus.ERROR,
            RunStatus.TIMEOUT,
            RunStatus.CANCELLED,
            RunStatus.INTERRUPTED,
        }:
            return _run_payload(run)
        await asyncio.sleep(0.05)


def _runtime_adapter(state: LocalRuntime) -> Any:
    adapters = state.adapters.all()
    if not adapters:
        raise RuntimeFailure(
            ErrorCode.CAPABILITY_NOT_SUPPORTED,
            "no runtime adapter is registered for this application",
        )
    return cast(Any, adapters[0])


def _compat_state(state: Any) -> JsonObject:
    if hasattr(state, "values"):
        values = cast(JsonValue, state.values)
        result: JsonObject = {
            "values": values,
            "next": list(getattr(state, "next", ())),
            "checkpoint": _checkpoint_from_config(getattr(state, "config", {})),
            "metadata": getattr(state, "metadata", {}),
            "created_at": getattr(state, "created_at", None),
            "parent_checkpoint": _checkpoint_from_config(getattr(state, "parent_config", None)),
            "tasks": list(getattr(state, "tasks", ())),
            "interrupts": list(getattr(state, "interrupts", ())),
        }
        return cast(JsonObject, jsonable_encoder(result))
    if isinstance(state, dict):
        return cast(JsonObject, state)
    return {"values": cast(JsonValue, state)}


def _checkpoint_from_config(config: Any) -> JsonObject | None:
    if not isinstance(config, dict):
        return None
    configurable = config.get("configurable")
    if not isinstance(configurable, dict):
        return None
    checkpoint_id = configurable.get("checkpoint_id")
    thread_id = configurable.get("thread_id")
    checkpoint_ns = configurable.get("checkpoint_ns", "")
    if checkpoint_id is None or thread_id is None:
        return None
    return {
        "thread_id": str(thread_id),
        "checkpoint_ns": str(checkpoint_ns),
        "checkpoint_id": str(checkpoint_id),
        "checkpoint_map": configurable.get("checkpoint_map", {}),
    }


def _identity(
    assistant_id: AssistantId, run_id: RunId, thread_id: ThreadId | None
) -> ExecutionIdentity:
    return ExecutionIdentity(
        ApplicationScope(
            WorkspaceId.parse("gateway"),
            ProjectId.parse("default"),
            ApplicationId.parse("gateway"),
            RevisionId.parse("active"),
            DeploymentId.parse("local"),
        ),
        assistant_id,
        run_id,
        AttemptId.new(),
        thread_id,
    )


def _sse_response(
    state: LocalRuntime,
    run_id: RunId,
    mode: str | list[str],
    *,
    request: Request,
    native: bool = False,
    after_sequence: int | None = None,
) -> StreamingResponse:
    selected_modes = {mode} if isinstance(mode, str) else set(mode)
    cursor_text = request.headers.get("last-event-id")
    if after_sequence is None:
        try:
            after_sequence = int(cursor_text) if cursor_text is not None else -1
        except ValueError as exc:
            raise RuntimeFailure(
                ErrorCode.STREAM_CURSOR_INVALID, "Last-Event-ID must be an integer"
            ) from exc
    if after_sequence < -1:
        raise RuntimeFailure(ErrorCode.STREAM_CURSOR_INVALID, "Last-Event-ID must be >= 0")

    async def stream() -> Any:
        if not native:
            yield "event: metadata\ndata: " + json.dumps({"run_id": str(run_id)}) + "\n\n"
        async for event in state.events.subscribe(run_id, after_sequence=after_sequence):
            if native:
                event_name = str(event.type)
                encoded_payload: JsonValue = _event_payload(event)
            else:
                stream_mode = event.native.get("langgraph_mode")
                if stream_mode not in selected_modes:
                    continue
                event_name = str(stream_mode)
                encoded_payload = event.data
            yield f"id: {event.sequence}\nevent: {event_name}\ndata: {json.dumps(encoded_payload, separators=(',', ':'))}\n\n"
        if not native:
            yield "event: end\ndata: null\n\n"

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


async def _event_cursor(state: LocalRuntime, run_id: RunId) -> int:
    events = await state.events.replay(run_id)
    return max((event.sequence for event in events), default=-1)


def _event_payload(event: Any) -> JsonObject:
    return {
        "schema_version": event.schema_version,
        "event_id": str(event.event_id),
        "sequence": event.sequence,
        "timestamp": event.timestamp.isoformat(),
        "application_id": str(event.identity.scope.application_id),
        "revision_id": str(event.identity.scope.revision_id),
        "deployment_id": str(event.identity.scope.deployment_id),
        "assistant_id": str(event.identity.assistant_id),
        "thread_id": str(event.identity.thread_id) if event.identity.thread_id else None,
        "run_id": str(event.identity.run_id),
        "attempt_id": str(event.identity.attempt_id),
        "type": str(event.type),
        "namespace": list(event.namespace),
        "data": event.data,
        "trace": {"trace_id": event.trace.trace_id, "span_id": event.trace.span_id},
        "native": event.native,
    }


def _native(data: Any, request: Request) -> NativeResponse:
    return NativeResponse(
        data=data,
        meta=NativeMeta(
            request_id=request.state.request_id,
            timestamp=datetime.now(UTC),
        ),
    )


def _load_schema() -> dict[str, Any]:
    import json as json_module

    schema_path = Path(os.environ.get("UR_CONTRACT_SCHEMA_PATH", str(SCHEMA_PATH)))
    return cast(dict[str, Any], json_module.loads(schema_path.read_text(encoding="utf-8")))


def _validation_errors(schema: dict[str, Any], payload: JsonObject) -> list[JsonObject]:
    return [
        {"path": list(error.absolute_path), "message": error.message}
        for error in Draft202012Validator(schema).iter_errors(payload)
    ]


def _revision_payload(revision: Any) -> JsonObject:
    return {
        "application_id": revision.application_id,
        "revision": revision.revision,
        "config": revision.config,
        "config_hash": revision.config_hash,
        "active": revision.active,
        "config_revision_id": str(revision.config_revision_id)
        if revision.config_revision_id
        else None,
    }


def _assistant_payload(assistant: Any) -> JsonObject:
    return {
        "assistant_id": str(assistant.assistant_id),
        "graph_id": assistant.graph_id,
        "version": assistant.version,
        "name": assistant.name,
        "config": assistant.config,
        "context": assistant.context,
        "metadata": assistant.metadata,
    }


def _thread_payload(thread: Any) -> JsonObject:
    return {
        "thread_id": str(thread.thread_id),
        "status": thread.status.value,
        "metadata": thread.metadata,
        "created_at": thread.created_at.isoformat() if thread.created_at else None,
        "updated_at": thread.updated_at.isoformat() if thread.updated_at else None,
    }


def _run_payload(run: Any) -> JsonObject:
    return {
        "run_id": str(run.run_id),
        "thread_id": str(run.thread_id) if run.thread_id else None,
        "assistant_id": str(run.identity.assistant_id),
        "status": run.status.value,
        "metadata": run.metadata,
    }


def _status_for(code: ErrorCode) -> int:
    return {
        ErrorCode.RESOURCE_NOT_FOUND: 404,
        ErrorCode.RUN_NOT_FOUND: 404,
        ErrorCode.THREAD_BUSY: 409,
        ErrorCode.INVALID_EXECUTION_INPUT: 422,
        ErrorCode.CAPABILITY_NOT_SUPPORTED: 501,
    }.get(code, 500)
