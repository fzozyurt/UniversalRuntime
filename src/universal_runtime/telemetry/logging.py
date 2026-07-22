from __future__ import annotations

import logging
import os
import sys

import structlog


def configure_logging() -> None:
    level_name = os.environ.get("UR_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    is_local = os.environ.get("UR_PROFILE", "local") == "local"

    timestamper = structlog.processors.TimeStamper(fmt="iso")

    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        timestamper,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]

    if is_local:
        processors = [*shared_processors, structlog.dev.ConsoleRenderer()]
    else:
        processors = [*shared_processors, structlog.processors.JSONRenderer()]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=level,
        force=True,
    )

    _redirect_uvicorn(structlog.get_logger("uvicorn"), level)
    _redirect_httpx(level)
    _redirect_aiokafka(level)


def _redirect_uvicorn(logger: structlog.stdlib.BoundLogger, level: int) -> None:
    uvicorn_logger = logging.getLogger("uvicorn")
    uvicorn_logger.handlers.clear()
    uvicorn_logger.propagate = False

    class StructlogHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            getattr(logger, record.levelname.lower(), logger.info)(
                record.getMessage(),
            )

    uvicorn_logger.addHandler(StructlogHandler())
    uvicorn_logger.setLevel(level)

    access = logging.getLogger("uvicorn.access")
    access.handlers.clear()
    access.propagate = False
    access.setLevel(level)

    class AccessHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            logger.info(
                "http.request",
                method=record.args.get("m", "?"),
                path=record.args.get("U", "?"),
                status=record.args.get("s", "?"),
                duration_ms=record.args.get("T", "?"),
            )

    access.addHandler(AccessHandler())


def _redirect_httpx(level: int) -> None:
    httpx_logger = logging.getLogger("httpx")
    httpx_logger.setLevel(logging.DEBUG if level <= logging.DEBUG else logging.WARNING)


def _redirect_aiokafka(level: int) -> None:
    for name in ("aiokafka", "kafka"):
        kafka_logger = logging.getLogger(name)
        kafka_logger.setLevel(level)
