from datetime import UTC, datetime, timedelta

import pytest

from universal_runtime.domain.identity import (
    ApplicationId,
    DeploymentId,
    LeaseId,
    RevisionId,
    RunId,
    WorkerId,
)
from universal_runtime.domain.workers import (
    WorkerLease,
    WorkerRegistration,
    WorkerStatus,
)


def test_worker_registration_rejects_capacity_overcommit() -> None:
    now = datetime.now(UTC)
    with pytest.raises(ValueError, match="exceed"):
        WorkerRegistration(
            worker_id=WorkerId.parse("worker-1"),
            application_id=ApplicationId.parse("application"),
            revision_id=RevisionId.parse("revision"),
            deployment_id=DeploymentId.parse("deployment"),
            grpc_target="worker-1:9090",
            graph_ids=frozenset({"graph-1"}),
            max_concurrency=4,
            active_executions=3,
            available_slots=2,
            status=WorkerStatus.READY,
            capabilities={},
            last_heartbeat_at=now,
            expires_at=now + timedelta(seconds=30),
        )


def test_worker_registration_supports_exact_deployment_graph_and_live_capacity() -> None:
    now = datetime.now(UTC)
    registration = WorkerRegistration(
        worker_id=WorkerId.parse("worker-1"),
        application_id=ApplicationId.parse("application"),
        revision_id=RevisionId.parse("revision"),
        deployment_id=DeploymentId.parse("deployment-blue"),
        grpc_target="worker-1:9090",
        graph_ids=frozenset({"graph-1", "graph-2"}),
        max_concurrency=4,
        active_executions=1,
        available_slots=3,
        status=WorkerStatus.READY,
        capabilities={},
        last_heartbeat_at=now,
        expires_at=now + timedelta(seconds=30),
    )

    assert registration.supports(
        DeploymentId.parse("deployment-blue"),
        "graph-2",
        now,
    )
    assert not registration.supports(
        DeploymentId.parse("deployment-green"),
        "graph-2",
        now,
    )
    assert not registration.supports(
        DeploymentId.parse("deployment-blue"),
        "missing",
        now,
    )
    assert not registration.supports(
        DeploymentId.parse("deployment-blue"),
        "graph-2",
        now + timedelta(seconds=31),
    )


def test_worker_lease_tracks_owner_target_and_expiry() -> None:
    now = datetime.now(UTC)
    lease = WorkerLease(
        lease_id=LeaseId.parse("lease-1"),
        worker_id=WorkerId.parse("worker-1"),
        run_id=RunId.parse("run-1"),
        grpc_target="worker-1:9090",
        expires_at=now + timedelta(seconds=60),
    )

    assert lease.is_active(now)
    assert not lease.is_active(now + timedelta(seconds=61))
    with pytest.raises(ValueError, match="grpc_target"):
        WorkerLease(
            lease_id=LeaseId.parse("lease-2"),
            worker_id=WorkerId.parse("worker-1"),
            run_id=RunId.parse("run-2"),
            grpc_target=" ",
            expires_at=now,
        )
