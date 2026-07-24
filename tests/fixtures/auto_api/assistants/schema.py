from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class AssistantCreate(BaseModel):
    name: str

    model_config = ConfigDict(json_schema_extra={"examples": [{"name": "Support"}]})


class AssistantRead(BaseModel):
    assistant_id: str
    name: str

    model_config = ConfigDict(
        json_schema_extra={"examples": [{"assistant_id": "assistant-1", "name": "Support"}]}
    )
