from __future__ import annotations

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import Response

from universal_runtime.transport.http.proxy import ProxyLimits, proxy_request


def create_custom_http_router(target: str, *, limits: ProxyLimits = ProxyLimits()) -> APIRouter:
    router = APIRouter(prefix="/api/v1/applications/{application_id}/http")
    client = httpx.AsyncClient()

    @router.api_route(
        "/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]
    )
    async def custom_http(application_id: str, path: str, request: Request) -> Response:
        del application_id
        response = await proxy_request(
            client,
            target=target,
            path=path,
            method=request.method,
            headers={key: value for key, value in request.headers.items()},
            body=await request.body(),
            query=request.url.query,
            limits=limits,
        )
        excluded = {"content-length", "transfer-encoding", "connection"}
        return Response(
            content=response.content,
            status_code=response.status_code,
            headers={k: v for k, v in response.headers.items() if k.lower() not in excluded},
            media_type=response.headers.get("content-type"),
        )

    return router
