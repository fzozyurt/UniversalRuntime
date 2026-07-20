from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import grpc
from google.protobuf import struct_pb2

from universal_runtime.adapters.grpc.generated.runtime.v1 import (
    execution_pb2,
    execution_pb2_grpc,
)
from universal_runtime.adapters.grpc.payloads import python_to_value, value_to_python
from universal_runtime.domain.events import RuntimeEventDraft, RuntimeEventType
from universal_runtime.domain.execution import RunCommand
from universal_runtime.domain.identity import ExecutionIdentity


def _struct(values: dict[str, Any]) -> struct_pb2.Struct:
    result = struct_pb2.Struct()
    result.update(values)
    return result


def _identity(identity: ExecutionIdentity) -> Any:
    return execution_pb2.ExecutionIdentity(
        workspace_id=str(identity.workspace_id),
        project_id=str(identity.project_id),
        application_id=str(identity.application_id),
        revision_id=str(identity.revision_id),
        deployment_id=str(identity.deployment_id),
        assistant_id=str(identity.assistant_id),
        thread_id=str(identity.thread_id) if identity.thread_id else "",
        run_id=str(identity.run_id),
        attempt_id=str(identity.attempt_id),
    )


def _invocation(command: RunCommand) -> Any:
    request = command.request
    return execution_pb2.InvokeRequest(
        identity=_identity(command.identity),
        target=execution_pb2.ExecutionTarget(
            graph_id=request.target.graph_id,
            assistant_version=request.target.assistant_version,
        ),
        input=python_to_value(request.input),
        command=python_to_value(request.command),
        config=_struct(request.config),
        context=_struct(request.context),
        metadata=_struct(request.metadata),
        stream_modes=list(request.stream_modes),
        stream_subgraphs=request.stream_subgraphs,
        priority=int(request.priority),
        timeout_seconds=request.timeout_seconds,
    )


class GrpcExecutionClient:
    def __init__(self) -> None:
        self._channels: dict[str, grpc.aio.Channel] = {}

    def _channel(self, target: str) -> grpc.aio.Channel:
        channel = self._channels.get(target)
        if channel is None:
            channel = grpc.aio.insecure_channel(target)
            self._channels[target] = channel
        return channel

    async def stream(
        self,
        target: str,
        command: RunCommand,
    ) -> AsyncIterator[RuntimeEventDraft]:
        request = command.request
        stub = execution_pb2_grpc.ExecutionServiceStub(self._channel(target))
        async for event in stub.Stream(
            execution_pb2.StreamRequest(invocation=_invocation(command)),
            timeout=request.timeout_seconds,
        ):
            yield RuntimeEventDraft(
                command.identity,
                RuntimeEventType(event.type),
                tuple(event.namespace),
                value_to_python(event.data),
                {
                    key: value_to_python(value)
                    for key, value in event.native.fields.items()
                },
            )

    async def close(self) -> None:
        for channel in self._channels.values():
            await channel.close()
        self._channels.clear()
