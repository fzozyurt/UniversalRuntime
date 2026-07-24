from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, RootModel


class JsonObjectResponse(RootModel[dict[str, Any]]):
    model_config = ConfigDict(
        json_schema_extra={"examples": [{"status": "ok"}]},
    )


class JsonListResponse(RootModel[list[dict[str, Any]]]):
    model_config = ConfigDict(
        json_schema_extra={"examples": [[{"id": "example"}]]},
    )


class HealthResponse(BaseModel):
    ok: bool = True

    model_config = ConfigDict(json_schema_extra={"examples": [{"ok": True}]})


class ReadinessResponse(BaseModel):
    ready: bool = True

    model_config = ConfigDict(json_schema_extra={"examples": [{"ready": True}]})


class AssistantResponse(BaseModel):
    assistant_id: str
    graph_id: str
    version: int = 1
    name: str | None = None
    description: str | None = None
    config: dict[str, Any] = Field(default_factory=dict)
    context: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "assistant_id": "support-agent",
                    "graph_id": "support-agent",
                    "version": 1,
                    "name": "Support Agent",
                    "description": "Answers customer support questions.",
                    "config": {},
                    "context": {},
                    "metadata": {"team": "support"},
                    "created_at": "2026-07-24T00:00:00Z",
                    "updated_at": "2026-07-24T00:00:00Z",
                }
            ]
        }
    )


class ThreadResponse(BaseModel):
    thread_id: str
    status: str
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None
    values: Any | None = None
    interrupts: Any | None = None

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "thread_id": "thread-01",
                    "status": "idle",
                    "metadata": {"customer_id": "customer-42"},
                    "created_at": "2026-07-24T00:00:00Z",
                    "updated_at": "2026-07-24T00:00:00Z",
                    "values": None,
                    "interrupts": None,
                }
            ]
        }
    )


class RunResponse(BaseModel):
    run_id: str
    thread_id: str | None = None
    assistant_id: str
    created_at: datetime | None = None
    updated_at: datetime | None = None
    status: str
    metadata: dict[str, Any] = Field(default_factory=dict)
    multitask_strategy: str = "reject"

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "run_id": "run-01",
                    "thread_id": "thread-01",
                    "assistant_id": "support-agent",
                    "created_at": "2026-07-24T00:00:00Z",
                    "updated_at": "2026-07-24T00:00:01Z",
                    "status": "success",
                    "metadata": {},
                    "multitask_strategy": "reject",
                }
            ]
        }
    )


class AssistantSchemasResponse(BaseModel):
    graph_id: str
    input_schema: dict[str, Any]
    output_schema: dict[str, Any]
    state_schema: dict[str, Any]
    config_schema: dict[str, Any]
    context_schema: dict[str, Any]


class PruneResponse(BaseModel):
    pruned_count: int

    model_config = ConfigDict(json_schema_extra={"examples": [{"pruned_count": 2}]})
