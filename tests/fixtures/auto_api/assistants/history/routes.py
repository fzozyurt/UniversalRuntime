from __future__ import annotations

from fastapi import APIRouter
from tests.fixtures.auto_api.assistants.history.schema import HistoryItem

router = APIRouter()


@router.get(
    "",
    response_model=list[HistoryItem],
    summary="List assistant history",
    description="List assistant history entries.",
)
async def list_history() -> list[HistoryItem]:
    return [HistoryItem(version=1, action="created")]
