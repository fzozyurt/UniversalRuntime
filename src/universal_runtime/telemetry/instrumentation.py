from __future__ import annotations

from typing import Any


def instrument_fastapi(app: Any) -> bool:
    try:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        FastAPIInstrumentor.instrument_app(app)
    except (ImportError, RuntimeError):
        return False
    return True


def instrument_clients() -> tuple[str, ...]:
    installed: list[str] = []
    for module_name, class_name, label in (
        ("opentelemetry.instrumentation.httpx", "HTTPXClientInstrumentor", "httpx"),
        ("opentelemetry.instrumentation.sqlalchemy", "SQLAlchemyInstrumentor", "sqlalchemy"),
        ("opentelemetry.instrumentation.grpc", "GrpcInstrumentorClient", "grpc"),
        ("opentelemetry.instrumentation.langchain", "LangchainInstrumentor", "langchain"),
    ):
        try:
            module = __import__(module_name, fromlist=[class_name])
            getattr(module, class_name)().instrument()
        except (ImportError, AttributeError, RuntimeError):
            continue
        installed.append(label)
    return tuple(installed)
