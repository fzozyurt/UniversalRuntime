from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

JsonObject = dict[str, Any]
StreamMode = str | list[str]


class TransportModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class NativeMeta(TransportModel):
    request_id: str | None = None
    timestamp: datetime | None = None


class NativeResponse(TransportModel):
    data: Any
    meta: NativeMeta


class RuntimeErrorBody(TransportModel):
    code: str
    message: str
    retryable: bool
    request_id: str
    details: JsonObject = Field(default_factory=dict)


class NativeErrorResponse(TransportModel):
    error: RuntimeErrorBody


class RuntimeInfo(TransportModel):
    version: str
    adapters: list[JsonObject]


class AssistantCreate(TransportModel):
    graph_id: str | None = None
    assistant_id: str | None = None
    name: str | None = None
    config: JsonObject = Field(default_factory=dict)
    context: JsonObject = Field(default_factory=dict)
    metadata: JsonObject = Field(default_factory=dict)


class Assistant(AssistantCreate):
    assistant_id: str
    graph_id: str
    version: int


class ThreadCreate(TransportModel):
    thread_id: str | None = None
    metadata: JsonObject = Field(default_factory=dict)


class Thread(TransportModel):
    thread_id: str
    status: Literal["idle", "busy", "interrupted", "error"]
    metadata: JsonObject = Field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None


class RunCreate(TransportModel):
    assistant_id: str
    input: Any = None
    command: Any = None
    config: JsonObject = Field(default_factory=dict)
    context: JsonObject = Field(default_factory=dict)
    metadata: JsonObject = Field(default_factory=dict)
    stream_mode: StreamMode = "values"
    stream_subgraphs: bool = False
    priority: Literal["interactive", "normal", "batch"] = "interactive"
    multitask_strategy: Literal["reject", "enqueue", "interrupt", "rollback"] = "reject"


class Run(TransportModel):
    run_id: str
    thread_id: str | None = None
    assistant_id: str
    status: Literal["pending", "running", "interrupted", "success", "error", "timeout", "cancelled"]
    metadata: JsonObject = Field(default_factory=dict)
