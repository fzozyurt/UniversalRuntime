from __future__ import annotations

from copy import deepcopy
from typing import Any

from universal_runtime.domain.execution import ExecutionRequest

PROTECTED = frozenset({"run_id", "thread_id", "checkpoint_ns", "checkpoint_id", "assistant_id"})


def map_config(request: ExecutionRequest) -> dict[str, Any]:
    identity = request.identity
    config: dict[str, Any] = deepcopy(request.config)
    configurable = dict(config.get("configurable", {}))
    metadata = dict(config.get("metadata", {}))
    # LangGraph's Postgres saver requires a checkpoint thread key even for the
    # SDK's stateless /runs and /runs/batch calls. Keep the public runtime
    # identity stateless, but isolate its checkpoint lineage by run_id.
    thread_id = str(identity.thread_id) if identity.thread_id is not None else str(identity.run_id)
    protected_config = {
        "thread_id": thread_id,
        "checkpoint_ns": configurable.get("checkpoint_ns", ""),
        "checkpoint_id": configurable.get("checkpoint_id"),
        "assistant_id": str(identity.assistant_id),
        "run_id": str(identity.run_id),
    }
    configurable.update(protected_config)
    protected_metadata = {
        "runtime.workspace_id": str(identity.scope.workspace_id),
        "runtime.project_id": str(identity.scope.project_id),
        "runtime.application_id": str(identity.scope.application_id),
        "runtime.revision_id": str(identity.scope.revision_id),
        "runtime.deployment_id": str(identity.scope.deployment_id),
        "runtime.assistant_id": str(identity.assistant_id),
        "runtime.thread_id": thread_id,
        "runtime.run_id": str(identity.run_id),
        "runtime.attempt_id": str(identity.attempt_id),
    }
    metadata.update(protected_metadata)
    config["run_id"] = str(identity.run_id)
    config["configurable"] = configurable
    config["metadata"] = metadata
    return config
