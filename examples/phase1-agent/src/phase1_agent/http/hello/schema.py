from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class HelloResponse(BaseModel):
    message: str

    model_config = ConfigDict(
        json_schema_extra={"examples": [{"message": "phase1-agent"}]}
    )
