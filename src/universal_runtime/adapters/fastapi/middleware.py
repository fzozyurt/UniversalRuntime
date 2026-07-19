from __future__ import annotations

from contextvars import ContextVar

from starlette.types import ASGIApp, Receive, Scope, Send

request_context: ContextVar[dict[str, str] | None] = ContextVar(
    "runtime_request_context", default=None
)


class RequestContextMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = {key.decode().lower(): value.decode() for key, value in scope.get("headers", [])}
        values = {
            key: headers[key]
            for key in ("x-request-id", "traceparent", "tracestate")
            if key in headers
        }
        token = request_context.set(values)
        try:
            await self.app(scope, receive, send)
        finally:
            request_context.reset(token)
