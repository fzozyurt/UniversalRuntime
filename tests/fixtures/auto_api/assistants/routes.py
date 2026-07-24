from __future__ import annotations

from fastapi import APIRouter, Body

from tests.fixtures.auto_api.assistants.schema import AssistantCreate, AssistantRead

router = APIRouter()


@router.post(
    "",
    response_model=AssistantRead,
    summary="Create assistant",
    description="Create an assistant in the fixture application.",
)
async def create_assistant(
    payload: AssistantCreate = Body(
        ...,
        openapi_examples={"support": {"value": {"name": "Support"}}},
    ),
) -> AssistantRead:
    return AssistantRead(assistant_id="assistant-1", name=payload.name)
