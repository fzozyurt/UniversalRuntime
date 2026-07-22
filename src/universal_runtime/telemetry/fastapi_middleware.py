from __future__ import annotations

import logging
import time

import structlog
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response


class RequestLogMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: object) -> None:
        super().__init__(app)  # type: ignore[arg-type]
        self._logger = structlog.get_logger("universal-runtime.http")
        self._debug = logging.getLogger().getEffectiveLevel() <= logging.DEBUG

    async def dispatch(self, request: Request, call_next: object) -> Response:
        start = time.monotonic()
        body = await request.body() if self._debug else None
        response = await call_next(request)  # type: ignore[arg-type]
        duration_ms = int((time.monotonic() - start) * 1000)
        log_kwargs: dict[str, object] = {
            "method": request.method,
            "path": request.url.path,
            "status": response.status_code,
            "duration_ms": duration_ms,
        }
        if self._debug:
            log_kwargs["request_body"] = body.decode("utf-8", errors="replace") if body else None
        self._logger.info("http.request", **log_kwargs)
        return response
