from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Index,
    Integer,
    MetaData,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from universal_runtime.adapters.postgres.schema import DEFAULT_SCHEMAS

NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s_%(column_1_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class PlatformBase(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class AuditMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    created_by: Mapped[str | None] = mapped_column(String(255))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )
    updated_by: Mapped[str | None] = mapped_column(String(255))
    row_version: Mapped[int] = mapped_column(Integer, server_default=text("1"), nullable=False)


class ApplicationRow(AuditMixin, PlatformBase):
    __tablename__ = "applications"
    __table_args__ = ({"schema": DEFAULT_SCHEMAS.core},)

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    environment: Mapped[str] = mapped_column(String(63), nullable=False)
    active_revision_id: Mapped[str | None] = mapped_column(String(255))


class ApplicationRevisionRow(AuditMixin, PlatformBase):
    __tablename__ = "application_revisions"
    __table_args__ = (
        UniqueConstraint("application_id", "revision_number"),
        UniqueConstraint("application_id", "config_hash"),
        {"schema": DEFAULT_SCHEMAS.core},
    )

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    application_id: Mapped[str] = mapped_column(String(255), nullable=False)
    revision_number: Mapped[int] = mapped_column(Integer, nullable=False)
    config: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    config_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))


class DeploymentRow(AuditMixin, PlatformBase):
    __tablename__ = "deployments"
    __table_args__ = ({"schema": DEFAULT_SCHEMAS.core},)

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    application_id: Mapped[str] = mapped_column(String(255), nullable=False)
    environment: Mapped[str] = mapped_column(String(63), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)


class DeploymentConfigRevisionRow(AuditMixin, PlatformBase):
    __tablename__ = "deployment_config_revisions"
    __table_args__ = (
        UniqueConstraint("deployment_id", "revision_number"),
        {"schema": DEFAULT_SCHEMAS.core},
    )

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    deployment_id: Mapped[str] = mapped_column(String(255), nullable=False)
    revision_number: Mapped[int] = mapped_column(Integer, nullable=False)
    config: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    config_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))


class GraphRow(AuditMixin, PlatformBase):
    __tablename__ = "graphs"
    __table_args__ = ({"schema": DEFAULT_SCHEMAS.core},)

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    application_id: Mapped[str] = mapped_column(String(255), nullable=False)
    graph_id: Mapped[str] = mapped_column(String(255), nullable=False)
    descriptor: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)


class AssistantRow(AuditMixin, PlatformBase):
    __tablename__ = "assistants"
    __table_args__ = ({"schema": DEFAULT_SCHEMAS.core},)

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    application_id: Mapped[str] = mapped_column(String(255), nullable=False)
    graph_id: Mapped[str] = mapped_column(String(255), nullable=False)
    active_version: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("1"))


class AssistantVersionRow(AuditMixin, PlatformBase):
    __tablename__ = "assistant_versions"
    __table_args__ = (
        UniqueConstraint("assistant_id", "version"),
        {"schema": DEFAULT_SCHEMAS.core},
    )

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    assistant_id: Mapped[str] = mapped_column(String(255), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    config: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    context: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column("metadata", JSON, nullable=False)


class ThreadRow(AuditMixin, PlatformBase):
    __tablename__ = "threads"
    __table_args__ = ({"schema": DEFAULT_SCHEMAS.execution},)

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column("metadata", JSON, nullable=False)


class RunRow(AuditMixin, PlatformBase):
    __tablename__ = "runs"
    __table_args__ = (
        Index(
            "uq_rt_exec_active_run_thread",
            "thread_id",
            unique=True,
            postgresql_where=text("status IN ('pending', 'running', 'interrupted')"),
        ),
        {"schema": DEFAULT_SCHEMAS.execution},
    )

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    workspace_id: Mapped[str] = mapped_column(String(255), nullable=False)
    project_id: Mapped[str] = mapped_column(String(255), nullable=False)
    application_id: Mapped[str] = mapped_column(String(255), nullable=False)
    revision_id: Mapped[str] = mapped_column(String(255), nullable=False)
    deployment_id: Mapped[str] = mapped_column(String(255), nullable=False)
    assistant_id: Mapped[str] = mapped_column(String(255), nullable=False)
    thread_id: Mapped[str | None] = mapped_column(String(255))
    attempt_id: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column("metadata", JSON, nullable=False)
    result: Mapped[Any | None] = mapped_column(JSON)
    error: Mapped[dict[str, Any] | None] = mapped_column(JSON)


class RunAttemptRow(AuditMixin, PlatformBase):
    __tablename__ = "run_attempts"
    __table_args__ = (
        UniqueConstraint("run_id", "attempt_number"),
        {"schema": DEFAULT_SCHEMAS.execution},
    )

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(255), nullable=False)
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    worker_id: Mapped[str | None] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(32), nullable=False)


class RunCommandRow(AuditMixin, PlatformBase):
    __tablename__ = "run_commands"
    __table_args__ = (
        UniqueConstraint("command_id"),
        {"schema": DEFAULT_SCHEMAS.execution},
    )

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    command_id: Mapped[str] = mapped_column(String(255), nullable=False)
    run_id: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    priority: Mapped[int] = mapped_column(Integer, nullable=False)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class ArtifactRow(AuditMixin, PlatformBase):
    __tablename__ = "artifacts"
    __table_args__ = ({"schema": DEFAULT_SCHEMAS.execution},)

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(255), nullable=False)
    kind: Mapped[str] = mapped_column(String(64), nullable=False)
    uri: Mapped[str] = mapped_column(String(2048), nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column("metadata", JSON, nullable=False)


class OutboxEventRow(AuditMixin, PlatformBase):
    __tablename__ = "outbox_events"
    __table_args__ = (
        UniqueConstraint("event_id"),
        UniqueConstraint("idempotency_key"),
        {"schema": DEFAULT_SCHEMAS.execution},
    )

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    event_id: Mapped[str] = mapped_column(String(255), nullable=False)
    aggregate_type: Mapped[str] = mapped_column(String(64), nullable=False)
    aggregate_id: Mapped[str] = mapped_column(String(255), nullable=False)
    topic: Mapped[str] = mapped_column(String(255), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class InboxEventRow(AuditMixin, PlatformBase):
    __tablename__ = "inbox_events"
    __table_args__ = (
        UniqueConstraint("consumer_name", "event_id"),
        {"schema": DEFAULT_SCHEMAS.execution},
    )

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    consumer_name: Mapped[str] = mapped_column(String(255), nullable=False)
    event_id: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class InterruptRow(AuditMixin, PlatformBase):
    __tablename__ = "interrupts"
    __table_args__ = ({"schema": DEFAULT_SCHEMAS.execution},)

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(255), nullable=False)
    namespace: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    resolved: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))


class RunLifecycleEventRow(AuditMixin, PlatformBase):
    __tablename__ = "run_lifecycle_events"
    __table_args__ = (
        UniqueConstraint("run_id", "sequence"),
        UniqueConstraint("event_id"),
        {"schema": DEFAULT_SCHEMAS.execution},
    )

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(255), nullable=False)
    event_id: Mapped[str] = mapped_column(String(255), nullable=False)
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)


class WorkerRow(AuditMixin, PlatformBase):
    __tablename__ = "workers"
    __table_args__ = ({"schema": DEFAULT_SCHEMAS.execution},)

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    worker_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    deployment_id: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    capabilities: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)


class WorkerLeaseRow(AuditMixin, PlatformBase):
    __tablename__ = "worker_leases"
    __table_args__ = ({"schema": DEFAULT_SCHEMAS.execution},)

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    lease_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    worker_id: Mapped[str] = mapped_column(String(255), nullable=False)
    run_id: Mapped[str] = mapped_column(String(255), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    acknowledged_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ApplicationMigrationRow(AuditMixin, PlatformBase):
    __tablename__ = "application_migrations"
    __table_args__ = (
        UniqueConstraint("application_id", "workspace_key", "environment"),
        {"schema": DEFAULT_SCHEMAS.core},
    )

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    application_id: Mapped[str] = mapped_column(String(255), nullable=False)
    workspace_key: Mapped[str] = mapped_column(String(255), nullable=False)
    environment: Mapped[str] = mapped_column(String(63), nullable=False)
    app_version: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(
        String(32), nullable=False, server_default=text("'pending'")
    )
    error: Mapped[str | None] = mapped_column(String(2048))
