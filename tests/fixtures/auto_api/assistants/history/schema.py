from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class HistoryItem(BaseModel):
    version: int
    action: str

    model_config = ConfigDict(json_schema_extra={"examples": [{"version": 1, "action": "created"}]})
