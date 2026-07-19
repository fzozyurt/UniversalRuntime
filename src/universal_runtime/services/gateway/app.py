from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from fastapi import Body, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, StreamingResponse
from jsonschema import Draft202012Validator

from universal_runtime.bootstrap.local import LocalRuntime, create_local_runtime
from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.domain.execution import ExecutionRequest, QueuePriority
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


def create_app(runtime: LocalRuntime | None = None) -> FastAPI:
    state = runtime or create_local_runtime()
    schema = _load_schema()
    app = FastAPI(title="UniversalRuntime Gateway", version="0.1.0")
    app.state.runtime = state

    @app.middleware("http")
    async def request_id(request: Request, call_next: Any) -> Any:
        request.state.request_id = request.headers.get("x-request-id") or str(CommandId.new())
        return await call_next(request)

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
                        details={"errors": cast(JsonObject, exc.errors())},
                    )
                ).model_dump(mode="json"),
            )
        return JSONResponse(status_code=422, content={"detail": exc.errors()})

    @app.get("/ok")
    async def health() -> dict[str, bool]:
        return {"ok": True}

    @app.get("/info")
    async def info() -> dict[str, Any]:
        return {
            "version": "0.1.0",
            "adapters": [
                adapter.manifest.__dict__ if hasattr(adapter.manifest, "__dict__") else {}
                for adapter in state.adapters.all()
            ],
        }

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

    @app.get("/assistants/{assistant_id}")
    async def get_assistant(assistant_id: str) -> dict[str, Any]:
        return _assistant_payload(await state.assistants.get(assistant_id))

    @app.post("/assistants/search")
    async def search_assistants() -> list[Any]:
        return [_assistant_payload(item) for item in await state.assistants.all()]

    @app.get("/assistants/{assistant_id}/schemas")
    async def assistant_schemas(assistant_id: str) -> dict[str, Any]:
        await state.assistants.get(assistant_id)
        return {"input_schema": None, "output_schema": None, "config_schema": None}

    @app.post("/threads")
    async def create_thread(payload: ThreadCreate | None = None) -> dict[str, Any]:
        body = payload or ThreadCreate()
        thread = await state.execution.create_thread(body.thread_id, body.metadata)
        return _thread_payload(thread)

    @app.get("/threads/{thread_id}/state")
    async def get_thread_state(thread_id: str) -> Any:
        await state.threads.get(thread_id)
        raise RuntimeFailure(
            ErrorCode.CAPABILITY_NOT_SUPPORTED, "thread state requires a registered runtime adapter"
        )

    @app.post("/threads/{thread_id}/state")
    async def update_thread_state(thread_id: str) -> Any:
        await state.threads.get(thread_id)
        raise RuntimeFailure(
            ErrorCode.CAPABILITY_NOT_SUPPORTED, "thread state requires a registered runtime adapter"
        )

    @app.get("/threads/{thread_id}/history")
    async def get_thread_history(thread_id: str) -> Any:
        await state.threads.get(thread_id)
        raise RuntimeFailure(
            ErrorCode.CAPABILITY_NOT_SUPPORTED,
            "thread history requires a registered runtime adapter",
        )

    @app.get("/threads/{thread_id}")
    async def get_thread(thread_id: str) -> dict[str, Any]:
        return _thread_payload(await state.threads.get(thread_id))

    @app.post("/threads/{thread_id}/runs")
    async def create_run(thread_id: str, payload: RunCreate) -> dict[str, Any]:
        run = await _start_run(state, payload, thread_id)
        return _run_payload(run)

    @app.post("/threads/{thread_id}/runs/stream")
    async def stream_run(thread_id: str, payload: RunCreate) -> StreamingResponse:
        run = await _start_run(state, payload, thread_id)
        return _sse_response(state, run.run_id, payload.stream_mode)

    @app.post("/runs/stream")
    async def stream_stateless_run(payload: RunCreate) -> StreamingResponse:
        run = await _start_run(state, payload, None)
        return _sse_response(state, run.run_id, payload.stream_mode)

    @app.post("/runs/wait")
    async def wait_stateless_run(payload: RunCreate) -> dict[str, Any]:
        run = await _start_run(state, payload, None)
        return _run_payload(run)

    @app.get("/runs/{run_id}")
    async def get_run(run_id: str) -> dict[str, Any]:
        return _run_payload(await state.runs.get(run_id))

    @app.post("/runs/{run_id}/cancel", status_code=204)
    async def cancel_run(run_id: str) -> None:
        await state.execution.cancel_run(run_id)

    return app


async def _start_run(state: LocalRuntime, payload: RunCreate, thread_id: str | None) -> Any:
    assistant = await state.assistants.get(payload.assistant_id)
    resolved_thread = ThreadId.parse(thread_id) if thread_id is not None else None
    identity = _identity(assistant.assistant_id, RunId.new(), resolved_thread)
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
    return await state.execution.start_run(request, outbox=state.outbox)


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


def _sse_response(state: LocalRuntime, run_id: RunId, mode: str | list[str]) -> StreamingResponse:
    selected = mode if isinstance(mode, str) else mode[0]

    async def stream() -> Any:
        events = await state.events.replay(run_id)
        yield (
            "event: metadata"
            + chr(10)
            + "data: "
            + json.dumps({"run_id": str(run_id)})
            + chr(10)
            + chr(10)
        )
        for event in events:
            yield (
                "event: "
                + selected
                + chr(10)
                + "data: "
                + json.dumps(event.data)
                + chr(10)
                + chr(10)
            )
        yield "event: end" + chr(10) + "data: null" + chr(10) + chr(10)

    return StreamingResponse(stream(), media_type="text/event-stream")


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

    return cast(dict[str, Any], json_module.loads(SCHEMA_PATH.read_text(encoding="utf-8")))


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
