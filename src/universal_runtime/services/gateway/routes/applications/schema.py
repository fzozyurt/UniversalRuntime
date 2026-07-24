from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from fastapi.routing import APIRoute
from pydantic import ConfigDict, RootModel


class ApplicationConfigRequest(RootModel[dict[str, Any]]):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "application": {"name": "support-runtime"},
                    "graphs": {"support-agent": "support.graph:graph"},
                    "persistence": {"mode": "platform-managed"},
                }
            ]
        }
    )


def response_model(route: APIRoute) -> Any | None:
    return route.response_model


def examples(route: APIRoute) -> Mapping[str, Any]:
    if "POST" in route.methods or "PUT" in route.methods:
        return {
            "application_config": {
                "summary": "Validate or save an application configuration",
                "value": {
                    "application": {"name": "support-runtime"},
                    "graphs": {"support-agent": "support.graph:graph"},
                    "persistence": {"mode": "platform-managed"},
                },
            }
        }
    return {}
