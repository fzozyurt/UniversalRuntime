import datetime

from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from universal_runtime.adapters.grpc.generated.runtime.v1 import execution_pb2 as _execution_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class WorkerCapabilities(_message.Message):
    __slots__ = ("adapter_id", "adapter_version", "profiles", "stream_modes", "checkpoint", "state", "history", "interrupt", "resume", "custom_http", "a2a", "session_affinity")
    ADAPTER_ID_FIELD_NUMBER: _ClassVar[int]
    ADAPTER_VERSION_FIELD_NUMBER: _ClassVar[int]
    PROFILES_FIELD_NUMBER: _ClassVar[int]
    STREAM_MODES_FIELD_NUMBER: _ClassVar[int]
    CHECKPOINT_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    HISTORY_FIELD_NUMBER: _ClassVar[int]
    INTERRUPT_FIELD_NUMBER: _ClassVar[int]
    RESUME_FIELD_NUMBER: _ClassVar[int]
    CUSTOM_HTTP_FIELD_NUMBER: _ClassVar[int]
    A2A_FIELD_NUMBER: _ClassVar[int]
    SESSION_AFFINITY_FIELD_NUMBER: _ClassVar[int]
    adapter_id: str
    adapter_version: str
    profiles: _containers.RepeatedScalarFieldContainer[str]
    stream_modes: _containers.RepeatedScalarFieldContainer[str]
    checkpoint: bool
    state: bool
    history: bool
    interrupt: bool
    resume: bool
    custom_http: bool
    a2a: bool
    session_affinity: str
    def __init__(self, adapter_id: _Optional[str] = ..., adapter_version: _Optional[str] = ..., profiles: _Optional[_Iterable[str]] = ..., stream_modes: _Optional[_Iterable[str]] = ..., checkpoint: _Optional[bool] = ..., state: _Optional[bool] = ..., history: _Optional[bool] = ..., interrupt: _Optional[bool] = ..., resume: _Optional[bool] = ..., custom_http: _Optional[bool] = ..., a2a: _Optional[bool] = ..., session_affinity: _Optional[str] = ...) -> None: ...

class RegisterWorkerRequest(_message.Message):
    __slots__ = ("worker_id", "application_id", "revision_id", "deployment_id", "pod_name", "max_concurrency", "config_hash", "capabilities", "metadata")
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    APPLICATION_ID_FIELD_NUMBER: _ClassVar[int]
    REVISION_ID_FIELD_NUMBER: _ClassVar[int]
    DEPLOYMENT_ID_FIELD_NUMBER: _ClassVar[int]
    POD_NAME_FIELD_NUMBER: _ClassVar[int]
    MAX_CONCURRENCY_FIELD_NUMBER: _ClassVar[int]
    CONFIG_HASH_FIELD_NUMBER: _ClassVar[int]
    CAPABILITIES_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    worker_id: str
    application_id: str
    revision_id: str
    deployment_id: str
    pod_name: str
    max_concurrency: int
    config_hash: str
    capabilities: WorkerCapabilities
    metadata: _struct_pb2.Struct
    def __init__(self, worker_id: _Optional[str] = ..., application_id: _Optional[str] = ..., revision_id: _Optional[str] = ..., deployment_id: _Optional[str] = ..., pod_name: _Optional[str] = ..., max_concurrency: _Optional[int] = ..., config_hash: _Optional[str] = ..., capabilities: _Optional[_Union[WorkerCapabilities, _Mapping]] = ..., metadata: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ...) -> None: ...

class RegisterWorkerResponse(_message.Message):
    __slots__ = ("accepted", "reason", "heartbeat_interval_seconds", "resolved_execution_defaults")
    ACCEPTED_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    HEARTBEAT_INTERVAL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    RESOLVED_EXECUTION_DEFAULTS_FIELD_NUMBER: _ClassVar[int]
    accepted: bool
    reason: str
    heartbeat_interval_seconds: int
    resolved_execution_defaults: _struct_pb2.Struct
    def __init__(self, accepted: _Optional[bool] = ..., reason: _Optional[str] = ..., heartbeat_interval_seconds: _Optional[int] = ..., resolved_execution_defaults: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ...) -> None: ...

class WorkerHeartbeat(_message.Message):
    __slots__ = ("worker_id", "timestamp", "active_executions", "available_slots")
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_EXECUTIONS_FIELD_NUMBER: _ClassVar[int]
    AVAILABLE_SLOTS_FIELD_NUMBER: _ClassVar[int]
    worker_id: str
    timestamp: _timestamp_pb2.Timestamp
    active_executions: int
    available_slots: int
    def __init__(self, worker_id: _Optional[str] = ..., timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., active_executions: _Optional[int] = ..., available_slots: _Optional[int] = ...) -> None: ...

class LeaseAccepted(_message.Message):
    __slots__ = ("lease_id", "run_id")
    LEASE_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    lease_id: str
    run_id: str
    def __init__(self, lease_id: _Optional[str] = ..., run_id: _Optional[str] = ...) -> None: ...

class LeaseRejected(_message.Message):
    __slots__ = ("lease_id", "run_id", "reason")
    LEASE_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    lease_id: str
    run_id: str
    reason: str
    def __init__(self, lease_id: _Optional[str] = ..., run_id: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class WorkerMessage(_message.Message):
    __slots__ = ("heartbeat", "lease_accepted", "lease_rejected", "event", "completion")
    HEARTBEAT_FIELD_NUMBER: _ClassVar[int]
    LEASE_ACCEPTED_FIELD_NUMBER: _ClassVar[int]
    LEASE_REJECTED_FIELD_NUMBER: _ClassVar[int]
    EVENT_FIELD_NUMBER: _ClassVar[int]
    COMPLETION_FIELD_NUMBER: _ClassVar[int]
    heartbeat: WorkerHeartbeat
    lease_accepted: LeaseAccepted
    lease_rejected: LeaseRejected
    event: _execution_pb2.RuntimeEvent
    completion: _execution_pb2.InvokeResponse
    def __init__(self, heartbeat: _Optional[_Union[WorkerHeartbeat, _Mapping]] = ..., lease_accepted: _Optional[_Union[LeaseAccepted, _Mapping]] = ..., lease_rejected: _Optional[_Union[LeaseRejected, _Mapping]] = ..., event: _Optional[_Union[_execution_pb2.RuntimeEvent, _Mapping]] = ..., completion: _Optional[_Union[_execution_pb2.InvokeResponse, _Mapping]] = ...) -> None: ...

class RunLease(_message.Message):
    __slots__ = ("lease_id", "expires_at", "invocation")
    LEASE_ID_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    INVOCATION_FIELD_NUMBER: _ClassVar[int]
    lease_id: str
    expires_at: _timestamp_pb2.Timestamp
    invocation: _execution_pb2.InvokeRequest
    def __init__(self, lease_id: _Optional[str] = ..., expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., invocation: _Optional[_Union[_execution_pb2.InvokeRequest, _Mapping]] = ...) -> None: ...

class CancelLease(_message.Message):
    __slots__ = ("run_id", "attempt_id", "reason")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    attempt_id: str
    reason: str
    def __init__(self, run_id: _Optional[str] = ..., attempt_id: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class ControllerMessage(_message.Message):
    __slots__ = ("run_lease", "cancel", "config_update", "drain")
    RUN_LEASE_FIELD_NUMBER: _ClassVar[int]
    CANCEL_FIELD_NUMBER: _ClassVar[int]
    CONFIG_UPDATE_FIELD_NUMBER: _ClassVar[int]
    DRAIN_FIELD_NUMBER: _ClassVar[int]
    run_lease: RunLease
    cancel: CancelLease
    config_update: _struct_pb2.Struct
    drain: bool
    def __init__(self, run_lease: _Optional[_Union[RunLease, _Mapping]] = ..., cancel: _Optional[_Union[CancelLease, _Mapping]] = ..., config_update: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., drain: _Optional[bool] = ...) -> None: ...

class MigrateRequest(_message.Message):
    __slots__ = ("application_id", "workspace_key", "environment", "app_version")
    APPLICATION_ID_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_KEY_FIELD_NUMBER: _ClassVar[int]
    ENVIRONMENT_FIELD_NUMBER: _ClassVar[int]
    APP_VERSION_FIELD_NUMBER: _ClassVar[int]
    application_id: str
    workspace_key: str
    environment: str
    app_version: str
    def __init__(self, application_id: _Optional[str] = ..., workspace_key: _Optional[str] = ..., environment: _Optional[str] = ..., app_version: _Optional[str] = ...) -> None: ...

class MigrateResponse(_message.Message):
    __slots__ = ("success", "error", "detail")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    DETAIL_FIELD_NUMBER: _ClassVar[int]
    success: bool
    error: str
    detail: str
    def __init__(self, success: _Optional[bool] = ..., error: _Optional[str] = ..., detail: _Optional[str] = ...) -> None: ...

class DrainWorkerRequest(_message.Message):
    __slots__ = ("worker_id", "timeout_seconds")
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_SECONDS_FIELD_NUMBER: _ClassVar[int]
    worker_id: str
    timeout_seconds: int
    def __init__(self, worker_id: _Optional[str] = ..., timeout_seconds: _Optional[int] = ...) -> None: ...
