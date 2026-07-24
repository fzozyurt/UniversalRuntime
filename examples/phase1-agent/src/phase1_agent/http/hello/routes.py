from __future__ import annotations

from fastapi import APIRouter

from phase1_agent.http.hello.schema import HelloResponse

router = APIRouter()


@router.get(
    "/",
    response_model=HelloResponse,
    summary="Say hello",
    description="Return a deterministic response from the Phase 1 agent application.",
)
async def hello() -> HelloResponse:
    return HelloResponse(message="phase1-agent")
