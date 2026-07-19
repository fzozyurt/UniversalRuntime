from __future__ import annotations

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from universal_runtime.adapters.fastapi.app_server import load_application_with_descriptor
from universal_runtime.adapters.fastapi.detector import detect_asgi_application
from universal_runtime.adapters.fastapi.middleware import request_context
from universal_runtime.domain.errors import ErrorCode, RuntimeFailure
from universal_runtime.transport.http.proxy import proxy_request


def test_explicit_detection_does_not_import_user_code(tmp_path) -> None:
    descriptor = detect_asgi_application(tmp_path, explicit_entrypoint="application:app")
    assert descriptor.entrypoint == "application:app"
    assert descriptor.detection_method == "explicit"


def test_ast_detection_finds_one_fastapi_application(tmp_path) -> None:
    (tmp_path / "application.py").write_text(
        "from fastapi import FastAPI\napp = FastAPI()\n", encoding="utf-8"
    )
    descriptor = detect_asgi_application(tmp_path)
    assert descriptor.entrypoint == "application:app"
    assert descriptor.detection_method == "ast"


def test_ast_detection_rejects_ambiguous_applications(tmp_path) -> None:
    source = "from fastapi import FastAPI\napp = FastAPI()\n"
    (tmp_path / "one.py").write_text(source, encoding="utf-8")
    (tmp_path / "two.py").write_text(source, encoding="utf-8")
    with pytest.raises(RuntimeFailure) as error:
        detect_asgi_application(tmp_path)
    assert error.value.code == ErrorCode.CUSTOM_HTTP_DISCOVERY_AMBIGUOUS


def test_ast_detection_reports_syntax_error(tmp_path) -> None:
    (tmp_path / "broken.py").write_text("app = FastAPI(\n", encoding="utf-8")
    with pytest.raises(RuntimeFailure) as error:
        detect_asgi_application(tmp_path)
    assert error.value.code == ErrorCode.ASGI_SYNTAX_ERROR


def test_isolated_detection_loads_factory_without_gateway_import(tmp_path) -> None:
    (tmp_path / "application.py").write_text(
        "from fastapi import FastAPI\n\ndef create_app():\n    return FastAPI()\n", encoding="utf-8"
    )
    descriptor = detect_asgi_application(tmp_path, isolated_import=True)
    assert descriptor.entrypoint == "application:create_app"
    assert descriptor.detection_method == "isolated_import"


def test_loaded_descriptor_and_root_path_and_context() -> None:
    app = FastAPI()

    @app.get("/hello", name="hello")
    async def hello() -> dict[str, str | None]:
        return {
            "request_id": request_context.get().get("x-request-id")
            if request_context.get()
            else None
        }

    import __main__

    __main__.app = app
    wrapped, descriptor = load_application_with_descriptor("__main__:app", root_path="/gateway")
    with TestClient(wrapped, root_path="/gateway") as client:
        response = client.get("/hello", headers={"x-request-id": "req-1"})
    assert response.json() == {"request_id": "req-1"}
    assert any(route.path == "/hello" for route in descriptor.routes)


@pytest.mark.asyncio
async def test_proxy_preserves_method_query_body_and_limits() -> None:
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update({"method": request.method, "url": str(request.url), "body": request.content})
        return httpx.Response(201, content=b"ok", headers={"content-type": "text/plain"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        response = await proxy_request(
            client,
            target="http://service",
            path="/echo",
            method="POST",
            headers={"x-request-id": "req-1"},
            body=b"payload",
            query="x=1",
        )
    assert response.status_code == 201
    assert seen == {"method": "POST", "url": "http://service/echo?x=1", "body": b"payload"}


@pytest.mark.asyncio
async def test_proxy_rejects_path_traversal() -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200))
    ) as client:
        with pytest.raises(RuntimeFailure) as error:
            await proxy_request(
                client,
                target="http://service",
                path="../secret",
                method="GET",
                headers={},
                body=b"",
            )
    assert error.value.code == ErrorCode.CUSTOM_HTTP_UNAVAILABLE
