from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class LauncherConfig:
    mode: str
    profile: str
    host: str
    port: int
    grpc_host: str
    grpc_port: int
    worker_max_concurrency: int
    worker_drain_timeout_seconds: float
    instance_id: str
    database_url: str | None
    kafka_bootstrap_servers: str | None
    topic_prefix: str
    kafka_environment: str
    observability_enabled: bool

    @classmethod
    def from_environment(
        cls,
        environ: dict[str, str] | None = None,
    ) -> LauncherConfig:
        values = environ or os.environ
        return cls(
            mode=values.get("UR_MODE", "worker"),
            profile=values.get("UR_PROFILE", "local"),
            host=values.get(
                "UR_GATEWAY_HOST",
                values.get("UR_HTTP_HOST", "0.0.0.0"),  # noqa: S104
            ),
            port=int(
                values.get(
                    "UR_GATEWAY_PORT",
                    values.get("UR_HTTP_PORT", "8080"),
                )
            ),
            grpc_host=values.get("UR_GRPC_HOST", "0.0.0.0"),  # noqa: S104
            grpc_port=int(values.get("UR_GRPC_PORT", "9090")),
            worker_max_concurrency=int(
                values.get("UR_WORKER_MAX_CONCURRENCY", "8")
            ),
            worker_drain_timeout_seconds=float(
                values.get("UR_WORKER_DRAIN_TIMEOUT_SECONDS", "30")
            ),
            instance_id=values.get("UR_INSTANCE_ID", "local"),
            database_url=(
                values.get("UR_PLATFORM_DATABASE_URL")
                or values.get("UR_DATABASE_URL")
            ),
            kafka_bootstrap_servers=values.get(
                "UR_KAFKA_BOOTSTRAP_SERVERS"
            ),
            topic_prefix=values.get(
                "UR_TOPIC_PREFIX",
                values.get("UR_KAFKA_TOPIC_PREFIX", "rt"),
            ),
            kafka_environment=values.get(
                "UR_KAFKA_ENVIRONMENT",
                values.get("UR_ENVIRONMENT", "local"),
            ),
            observability_enabled=(
                values.get("UR_OBSERVABILITY_ENABLED", "false").lower()
                == "true"
            ),
        )

    def require_database_url(self) -> str:
        if not self.database_url:
            raise ValueError(
                "UR_PLATFORM_DATABASE_URL or UR_DATABASE_URL is required"
            )
        return self.database_url

    def require_kafka_bootstrap_servers(self) -> str:
        if not self.kafka_bootstrap_servers:
            raise ValueError("UR_KAFKA_BOOTSTRAP_SERVERS is required")
        return self.kafka_bootstrap_servers

    def validate(self) -> None:
        if self.mode not in {
            "all",
            "worker",
            "api",
            "gateway",
            "dispatcher",
            "outbox-relay",
            "projector",
            "standalone",
            "inspect",
            "migrate",
        }:
            raise ValueError(f"unsupported UR_MODE: {self.mode}")
        if not 1 <= self.port <= 65535 or not 1 <= self.grpc_port <= 65535:
            raise ValueError("service ports must be between 1 and 65535")
        if self.worker_max_concurrency < 1:
            raise ValueError("UR_WORKER_MAX_CONCURRENCY must be positive")
        if self.worker_drain_timeout_seconds < 0:
            raise ValueError(
                "UR_WORKER_DRAIN_TIMEOUT_SECONDS must not be negative"
            )
