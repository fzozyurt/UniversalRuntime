from __future__ import annotations

from dataclasses import dataclass

import httpx

from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.transport.http.forwarded_headers import trusted_forwarded_headers


@dataclass(frozen=True, slots=True)
class ProxyLimits:
    max_request_bytes: int = 10 * 1024 * 1024
    max_response_bytes: int = 50 * 1024 * 1024
    timeout_seconds: float = 30.0


def safe_proxy_path(path: str) -> str:
    if ".." in path.split("/"):
        raise RuntimeFailure(ErrorCode.CUSTOM_HTTP_UNAVAILABLE, "path traversal is not allowed")
    return "/" + path.lstrip("/")


async def proxy_request(
    client: httpx.AsyncClient,
    *,
    target: str,
    path: str,
    method: str,
    headers: dict[str, str],
    body: bytes,
    query: str = "",
    trusted_proxy: bool = True,
    limits: ProxyLimits = ProxyLimits(),
) -> httpx.Response:
    if len(body) > limits.max_request_bytes:
        raise RuntimeFailure(
            ErrorCode.CUSTOM_HTTP_RESPONSE_TOO_LARGE, "request body exceeded limit"
        )
    forwarded = trusted_forwarded_headers(headers, trusted=trusted_proxy)
    url = target.rstrip("/") + safe_proxy_path(path)
    if query:
        url += "?" + query
    try:
        response = await client.request(
            method, url, headers=forwarded, content=body, timeout=limits.timeout_seconds
        )
    except httpx.TimeoutException as exc:
        raise RuntimeFailure(
            ErrorCode.CUSTOM_HTTP_TIMEOUT, "custom HTTP request timed out", retryable=True
        ) from exc
    except httpx.HTTPError as exc:
        raise RuntimeFailure(
            ErrorCode.CUSTOM_HTTP_UNAVAILABLE, "custom HTTP target is unavailable", retryable=True
        ) from exc
    content = await response.aread()
    if len(content) > limits.max_response_bytes:
        raise RuntimeFailure(
            ErrorCode.CUSTOM_HTTP_RESPONSE_TOO_LARGE, "response body exceeded limit"
        )
    response._content = content
    return response
