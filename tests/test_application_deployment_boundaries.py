from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from universal_runtime.adapters.memory.events import InMemoryEventJournal
from universal_runtime.adapters.memory.queue import InMemoryPriorityQueue
from universal_runtime.adapters.memory.repositories import (
    InMemoryRunRepository,
    InMemoryThreadRepository,
)
from universal_runtime.application.bound_execution_service import (
    ApplicationBoundExecutionService,
)
from universal_runtime.domain.applications import ResolvedExecutionPlan
from universal_runtime.domain.assistants import Assistant
from universal_runtime.domain.execution import (
    ExecutionRequest,
    ExecutionTarget,
    Run,
    RunStatus,
)
from universal_runtime.domain.identity import (
    ApplicationId,
    ApplicationScope,
    AssistantId,
    AttemptId,
    DeploymentId,
    ExecutionIdentity,
    LeaseId,
    ProjectId,
    RevisionId,
    RunId,
    ThreadId,
    WorkerId,
    WorkspaceId,
)
from universal_runtime.domain.workers import (
    WorkerLease,
    WorkerRegistration,
    WorkerStatus,
)


def _scope(
    application: str = "application-a",
    revision: str = "revision-a",
    deployment: str = "deployment-a",
) -> ApplicationScope:
    return ApplicationScope(
        WorkspaceId.parse("workspace"),
        ProjectId.parse("project"),
        ApplicationId.parse(application),
        RevisionId.parse(revision),
        DeploymentId.parse(deployment),
    )


def _identity(scope: ApplicationScope, thread_id: ThreadId | None) -> ExecutionIdentity:
    return ExecutionIdentity(
        scope,
        AssistantId.parse("assistant-custom"),
        RunId.new(),
        AttemptId.new(),
        thread_id,
    )


class FakePlanResolver:
    def __init__(self, plan: ResolvedExecutionPlan) -> None:
        self.plan = plan
        self.requested_versions: list[int | None] = []

    async def resolve(
        self,
        assistant_id: AssistantId,
        *,
        version: int | None = None,
    ) -> ResolvedExecutionPlan:
        assert assistant_id == self.plan.assistant.assistant_id
        self.requested_versions.append(version)
        return self.plan


class FakeThreadBinder:
    def __init__(self) -> None:
        self.calls: list[tuple[str, ApplicationScope]] = []

    async def bind(self, thread_id: str, scope: ApplicationScope) -> None:
        self.calls.append((thread_id, scope))


@pytest.mark.asyncio
async def test_execution_plan_pins_scope_target_and_assistant_configuration() -> None:
    threads = InMemoryThreadRepository()
    runs = InMemoryRunRepository()
    events = InMemoryEventJournal()
    commands = InMemoryPriorityQueue()
    assistant = Assistant(
        assistant_id=AssistantId.parse("assistant-custom"),
        graph_id="graph-a",
        version=7,
        config={"model": "assistant-model", "temperature": 0},
        context={"tenant": "assistant-context"},
        metadata={"source": "assistant"},
    )
    real_scope = _scope()
    resolver = FakePlanResolver(
        ResolvedExecutionPlan(
            real_scope,
            assistant,
            ExecutionTarget("graph-a", 7),
        )
    )
    binder = FakeThreadBinder()
    service = ApplicationBoundExecutionService(
        thread_binder=binder,
        threads=threads,
        runs=runs,
        commands=commands,
        journal=events,
        replay=events,
        subscription=events,
        plan_resolver=resolver,
    )
    thread = await service.create_thread()
    placeholder_scope = _scope("gateway", "active", "local")
    request = ExecutionRequest(
        identity=_identity(placeholder_scope, thread.thread_id),
        input={"messages": [{"role": "user", "content": "hello"}]},
        config={"temperature": 0.2},
        context={"request": "context"},
        metadata={"request_id": "request-a"},
    )

    run = await service.start_run(request)
    receipt = await commands.receive(WorkerId.parse("worker"))

    assert run.identity.scope == real_scope
    assert run.target == ExecutionTarget("graph-a", 7)
    assert binder.calls == [(str(thread.thread_id), real_scope)]
    assert receipt.command.request.target == ExecutionTarget("graph-a", 7)
    assert receipt.command.request.config == {
        "model": "assistant-model",
        "temperature": 0.2,
    }
    assert receipt.command.request.context == {
        "tenant": "assistant-context",
        "request": "context",
    }
    assert receipt.command.request.metadata == {
        "source": "assistant",
        "request_id": "request-a",
    }
    assert resolver.requested_versions == [None]


@pytest.mark.asyncio
async def test_explicit_graph_must_match_resolved_assistant_graph() -> None:
    events = InMemoryEventJournal()
    resolver = FakePlanResolver(
        ResolvedExecutionPlan(
            _scope(),
            Assistant(
                assistant_id=AssistantId.parse("assistant-custom"),
                graph_id="graph-a",
                version=1,
            ),
            ExecutionTarget("graph-a", 1),
        )
    )
    service = ApplicationBoundExecutionService(
        thread_binder=FakeThreadBinder(),
        threads=InMemoryThreadRepository(),
        runs=InMemoryRunRepository(),
        commands=InMemoryPriorityQueue(),
        journal=events,
        replay=events,
        subscription=events,
        plan_resolver=resolver,
    )

    with pytest.raises(Exception, match="does not match assistant graph"):
        await service.start_run(
            ExecutionRequest(
                identity=_identity(_scope("gateway"), None),
                target=ExecutionTarget("graph-b", 1),
            )
        )


def test_run_transitions_preserve_execution_target() -> None:
    now = datetime.now(UTC)
    target = ExecutionTarget("graph-a", 4)
    run = Run(
        identity=_identity(_scope(), None),
        created_at=now,
        updated_at=now,
        target=target,
    )

    running = run.mark_running(now + timedelta(seconds=1))
    pending = running.requeue(now + timedelta(seconds=2))
    completed = pending.mark_running(now + timedelta(seconds=3)).complete(
        {"ok": True},
        now + timedelta(seconds=4),
    )

    assert running.target == target
    assert pending.status is RunStatus.PENDING
    assert pending.target == target
    assert completed.target == target


def test_worker_registration_and_lease_enforce_capacity_contracts() -> None:
    now = datetime.now(UTC)
    registration = WorkerRegistration(
        worker_id=WorkerId.parse("worker-a"),
        application_id=ApplicationId.parse("application-a"),
        revision_id=RevisionId.parse("revision-a"),
        deployment_id=DeploymentId.parse("deployment-a"),
        grpc_target="worker-a:9090",
        graph_ids=frozenset({"graph-a"}),
        max_concurrency=4,
        active_executions=1,
        available_slots=3,
        status=WorkerStatus.READY,
        capabilities={},
        last_heartbeat_at=now,
        expires_at=now + timedelta(seconds=45),
    )
    lease = WorkerLease(
        lease_id=LeaseId.new(),
        worker_id=registration.worker_id,
        run_id=RunId.new(),
        deployment_id=registration.deployment_id,
        graph_id="graph-a",
        grpc_target=registration.grpc_target,
        leased_at=now,
        expires_at=now + timedelta(seconds=30),
    )

    assert registration.supports(
        DeploymentId.parse("deployment-a"),
        "graph-a",
        now,
    )
    assert not registration.supports(
        DeploymentId.parse("deployment-b"),
        "graph-a",
        now,
    )
    assert lease.graph_id == "graph-a"

    with pytest.raises(ValueError, match="capacity counters"):
        WorkerRegistration(
            worker_id=WorkerId.parse("worker-b"),
            application_id=ApplicationId.parse("application-a"),
            revision_id=RevisionId.parse("revision-a"),
            deployment_id=DeploymentId.parse("deployment-a"),
            grpc_target="worker-b:9090",
            graph_ids=frozenset({"graph-a"}),
            max_concurrency=2,
            active_executions=2,
            available_slots=1,
            status=WorkerStatus.BUSY,
            capabilities={},
            last_heartbeat_at=now,
            expires_at=now + timedelta(seconds=45),
        )
