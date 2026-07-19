from __future__ import annotations

from starlette.types import ASGIApp, Receive, Scope, Send


class RootPathMiddleware:
    def __init__(self, app: ASGIApp, root_path: str) -> None:
        self.app = app
        self.root_path = "/" + root_path.strip("/") if root_path.strip("/") else ""

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] in {"http", "websocket"}:
            scope = dict(scope)
            scope["root_path"] = self.root_path
        await self.app(scope, receive, send)
