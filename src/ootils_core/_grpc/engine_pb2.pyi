import datetime

from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class PropagateRequest(_message.Message):
    __slots__ = ("scenario_id", "event_id", "event_type", "trigger_node_id", "payload")
    SCENARIO_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    TRIGGER_NODE_ID_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    scenario_id: str
    event_id: str
    event_type: str
    trigger_node_id: str
    payload: bytes
    def __init__(self, scenario_id: _Optional[str] = ..., event_id: _Optional[str] = ..., event_type: _Optional[str] = ..., trigger_node_id: _Optional[str] = ..., payload: _Optional[bytes] = ...) -> None: ...

class PropagateResponse(_message.Message):
    __slots__ = ("calc_run_id", "nodes_processed", "nodes_changed", "shortages_detected", "timing")
    CALC_RUN_ID_FIELD_NUMBER: _ClassVar[int]
    NODES_PROCESSED_FIELD_NUMBER: _ClassVar[int]
    NODES_CHANGED_FIELD_NUMBER: _ClassVar[int]
    SHORTAGES_DETECTED_FIELD_NUMBER: _ClassVar[int]
    TIMING_FIELD_NUMBER: _ClassVar[int]
    calc_run_id: str
    nodes_processed: int
    nodes_changed: int
    shortages_detected: int
    timing: EngineTiming
    def __init__(self, calc_run_id: _Optional[str] = ..., nodes_processed: _Optional[int] = ..., nodes_changed: _Optional[int] = ..., shortages_detected: _Optional[int] = ..., timing: _Optional[_Union[EngineTiming, _Mapping]] = ...) -> None: ...

class EngineTiming(_message.Message):
    __slots__ = ("dirty_expand_us", "compute_us", "shortage_detect_us", "wal_fsync_us", "total_us")
    DIRTY_EXPAND_US_FIELD_NUMBER: _ClassVar[int]
    COMPUTE_US_FIELD_NUMBER: _ClassVar[int]
    SHORTAGE_DETECT_US_FIELD_NUMBER: _ClassVar[int]
    WAL_FSYNC_US_FIELD_NUMBER: _ClassVar[int]
    TOTAL_US_FIELD_NUMBER: _ClassVar[int]
    dirty_expand_us: float
    compute_us: float
    shortage_detect_us: float
    wal_fsync_us: float
    total_us: float
    def __init__(self, dirty_expand_us: _Optional[float] = ..., compute_us: _Optional[float] = ..., shortage_detect_us: _Optional[float] = ..., wal_fsync_us: _Optional[float] = ..., total_us: _Optional[float] = ...) -> None: ...

class ForkRequest(_message.Message):
    __slots__ = ("parent_scenario_id", "name")
    PARENT_SCENARIO_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    parent_scenario_id: str
    name: str
    def __init__(self, parent_scenario_id: _Optional[str] = ..., name: _Optional[str] = ...) -> None: ...

class MergeRequest(_message.Message):
    __slots__ = ("scenario_id", "target_scenario_id")
    SCENARIO_ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_SCENARIO_ID_FIELD_NUMBER: _ClassVar[int]
    scenario_id: str
    target_scenario_id: str
    def __init__(self, scenario_id: _Optional[str] = ..., target_scenario_id: _Optional[str] = ...) -> None: ...

class MergeResult(_message.Message):
    __slots__ = ("nodes_merged", "new_baseline_generation")
    NODES_MERGED_FIELD_NUMBER: _ClassVar[int]
    NEW_BASELINE_GENERATION_FIELD_NUMBER: _ClassVar[int]
    nodes_merged: int
    new_baseline_generation: str
    def __init__(self, nodes_merged: _Optional[int] = ..., new_baseline_generation: _Optional[str] = ...) -> None: ...

class ScenarioInfo(_message.Message):
    __slots__ = ("id", "name", "parent_id", "created_at", "overlay_size", "memory_bytes")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    PARENT_ID_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    OVERLAY_SIZE_FIELD_NUMBER: _ClassVar[int]
    MEMORY_BYTES_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    parent_id: str
    created_at: _timestamp_pb2.Timestamp
    overlay_size: int
    memory_bytes: int
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., parent_id: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., overlay_size: _Optional[int] = ..., memory_bytes: _Optional[int] = ...) -> None: ...

class ScenarioList(_message.Message):
    __slots__ = ("scenarios",)
    SCENARIOS_FIELD_NUMBER: _ClassVar[int]
    scenarios: _containers.RepeatedCompositeFieldContainer[ScenarioInfo]
    def __init__(self, scenarios: _Optional[_Iterable[_Union[ScenarioInfo, _Mapping]]] = ...) -> None: ...

class NodeQuery(_message.Message):
    __slots__ = ("scenario_id", "node_id")
    SCENARIO_ID_FIELD_NUMBER: _ClassVar[int]
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    scenario_id: str
    node_id: str
    def __init__(self, scenario_id: _Optional[str] = ..., node_id: _Optional[str] = ...) -> None: ...

class NodeState(_message.Message):
    __slots__ = ("node_id", "node_type", "item_id", "location_id", "opening_stock", "inflows", "outflows", "closing_stock", "has_shortage", "shortage_qty", "time_span_start", "time_span_end", "bucket_sequence")
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    NODE_TYPE_FIELD_NUMBER: _ClassVar[int]
    ITEM_ID_FIELD_NUMBER: _ClassVar[int]
    LOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    OPENING_STOCK_FIELD_NUMBER: _ClassVar[int]
    INFLOWS_FIELD_NUMBER: _ClassVar[int]
    OUTFLOWS_FIELD_NUMBER: _ClassVar[int]
    CLOSING_STOCK_FIELD_NUMBER: _ClassVar[int]
    HAS_SHORTAGE_FIELD_NUMBER: _ClassVar[int]
    SHORTAGE_QTY_FIELD_NUMBER: _ClassVar[int]
    TIME_SPAN_START_FIELD_NUMBER: _ClassVar[int]
    TIME_SPAN_END_FIELD_NUMBER: _ClassVar[int]
    BUCKET_SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    node_id: str
    node_type: str
    item_id: str
    location_id: str
    opening_stock: str
    inflows: str
    outflows: str
    closing_stock: str
    has_shortage: bool
    shortage_qty: str
    time_span_start: str
    time_span_end: str
    bucket_sequence: int
    def __init__(self, node_id: _Optional[str] = ..., node_type: _Optional[str] = ..., item_id: _Optional[str] = ..., location_id: _Optional[str] = ..., opening_stock: _Optional[str] = ..., inflows: _Optional[str] = ..., outflows: _Optional[str] = ..., closing_stock: _Optional[str] = ..., has_shortage: bool = ..., shortage_qty: _Optional[str] = ..., time_span_start: _Optional[str] = ..., time_span_end: _Optional[str] = ..., bucket_sequence: _Optional[int] = ...) -> None: ...

class ShortagesQuery(_message.Message):
    __slots__ = ("scenario_id", "item_id", "location_id", "severity_class")
    SCENARIO_ID_FIELD_NUMBER: _ClassVar[int]
    ITEM_ID_FIELD_NUMBER: _ClassVar[int]
    LOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    SEVERITY_CLASS_FIELD_NUMBER: _ClassVar[int]
    scenario_id: str
    item_id: str
    location_id: str
    severity_class: str
    def __init__(self, scenario_id: _Optional[str] = ..., item_id: _Optional[str] = ..., location_id: _Optional[str] = ..., severity_class: _Optional[str] = ...) -> None: ...

class Shortage(_message.Message):
    __slots__ = ("shortage_id", "pi_node_id", "item_id", "location_id", "shortage_date", "shortage_qty", "severity_score", "severity_class")
    SHORTAGE_ID_FIELD_NUMBER: _ClassVar[int]
    PI_NODE_ID_FIELD_NUMBER: _ClassVar[int]
    ITEM_ID_FIELD_NUMBER: _ClassVar[int]
    LOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    SHORTAGE_DATE_FIELD_NUMBER: _ClassVar[int]
    SHORTAGE_QTY_FIELD_NUMBER: _ClassVar[int]
    SEVERITY_SCORE_FIELD_NUMBER: _ClassVar[int]
    SEVERITY_CLASS_FIELD_NUMBER: _ClassVar[int]
    shortage_id: str
    pi_node_id: str
    item_id: str
    location_id: str
    shortage_date: str
    shortage_qty: str
    severity_score: str
    severity_class: str
    def __init__(self, shortage_id: _Optional[str] = ..., pi_node_id: _Optional[str] = ..., item_id: _Optional[str] = ..., location_id: _Optional[str] = ..., shortage_date: _Optional[str] = ..., shortage_qty: _Optional[str] = ..., severity_score: _Optional[str] = ..., severity_class: _Optional[str] = ...) -> None: ...

class StreamRequest(_message.Message):
    __slots__ = ("scenario_id", "include_baseline_changes")
    SCENARIO_ID_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_BASELINE_CHANGES_FIELD_NUMBER: _ClassVar[int]
    scenario_id: str
    include_baseline_changes: bool
    def __init__(self, scenario_id: _Optional[str] = ..., include_baseline_changes: bool = ...) -> None: ...

class ChangeEvent(_message.Message):
    __slots__ = ("timestamp", "scenario_id", "calc_run_id", "node_updated", "shortage_detected", "scenario_forked")
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    SCENARIO_ID_FIELD_NUMBER: _ClassVar[int]
    CALC_RUN_ID_FIELD_NUMBER: _ClassVar[int]
    NODE_UPDATED_FIELD_NUMBER: _ClassVar[int]
    SHORTAGE_DETECTED_FIELD_NUMBER: _ClassVar[int]
    SCENARIO_FORKED_FIELD_NUMBER: _ClassVar[int]
    timestamp: _timestamp_pb2.Timestamp
    scenario_id: str
    calc_run_id: str
    node_updated: NodeUpdated
    shortage_detected: ShortageDetected
    scenario_forked: ScenarioForked
    def __init__(self, timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., scenario_id: _Optional[str] = ..., calc_run_id: _Optional[str] = ..., node_updated: _Optional[_Union[NodeUpdated, _Mapping]] = ..., shortage_detected: _Optional[_Union[ShortageDetected, _Mapping]] = ..., scenario_forked: _Optional[_Union[ScenarioForked, _Mapping]] = ...) -> None: ...

class NodeUpdated(_message.Message):
    __slots__ = ("node_id", "closing_stock")
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    CLOSING_STOCK_FIELD_NUMBER: _ClassVar[int]
    node_id: str
    closing_stock: str
    def __init__(self, node_id: _Optional[str] = ..., closing_stock: _Optional[str] = ...) -> None: ...

class ShortageDetected(_message.Message):
    __slots__ = ("pi_node_id", "shortage_qty")
    PI_NODE_ID_FIELD_NUMBER: _ClassVar[int]
    SHORTAGE_QTY_FIELD_NUMBER: _ClassVar[int]
    pi_node_id: str
    shortage_qty: str
    def __init__(self, pi_node_id: _Optional[str] = ..., shortage_qty: _Optional[str] = ...) -> None: ...

class ScenarioForked(_message.Message):
    __slots__ = ("parent_id", "new_id")
    PARENT_ID_FIELD_NUMBER: _ClassVar[int]
    NEW_ID_FIELD_NUMBER: _ClassVar[int]
    parent_id: str
    new_id: str
    def __init__(self, parent_id: _Optional[str] = ..., new_id: _Optional[str] = ...) -> None: ...

class HealthStatus(_message.Message):
    __slots__ = ("status", "detail", "boot_time", "uptime_seconds")
    class Status(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        UNKNOWN: _ClassVar[HealthStatus.Status]
        SERVING: _ClassVar[HealthStatus.Status]
        DEGRADED: _ClassVar[HealthStatus.Status]
        NOT_SERVING: _ClassVar[HealthStatus.Status]
    UNKNOWN: HealthStatus.Status
    SERVING: HealthStatus.Status
    DEGRADED: HealthStatus.Status
    NOT_SERVING: HealthStatus.Status
    STATUS_FIELD_NUMBER: _ClassVar[int]
    DETAIL_FIELD_NUMBER: _ClassVar[int]
    BOOT_TIME_FIELD_NUMBER: _ClassVar[int]
    UPTIME_SECONDS_FIELD_NUMBER: _ClassVar[int]
    status: HealthStatus.Status
    detail: str
    boot_time: _timestamp_pb2.Timestamp
    uptime_seconds: int
    def __init__(self, status: _Optional[_Union[HealthStatus.Status, str]] = ..., detail: _Optional[str] = ..., boot_time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., uptime_seconds: _Optional[int] = ...) -> None: ...

class EngineMetrics(_message.Message):
    __slots__ = ("baseline_graph_bytes", "total_scenarios_bytes", "active_scenarios", "events_processed_total", "nodes_recomputed_total", "shortages_detected_total", "propagate_p50_us", "propagate_p95_us", "propagate_p99_us", "pg_writeback_queue_depth", "wal_size_bytes", "last_pg_flush")
    BASELINE_GRAPH_BYTES_FIELD_NUMBER: _ClassVar[int]
    TOTAL_SCENARIOS_BYTES_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_SCENARIOS_FIELD_NUMBER: _ClassVar[int]
    EVENTS_PROCESSED_TOTAL_FIELD_NUMBER: _ClassVar[int]
    NODES_RECOMPUTED_TOTAL_FIELD_NUMBER: _ClassVar[int]
    SHORTAGES_DETECTED_TOTAL_FIELD_NUMBER: _ClassVar[int]
    PROPAGATE_P50_US_FIELD_NUMBER: _ClassVar[int]
    PROPAGATE_P95_US_FIELD_NUMBER: _ClassVar[int]
    PROPAGATE_P99_US_FIELD_NUMBER: _ClassVar[int]
    PG_WRITEBACK_QUEUE_DEPTH_FIELD_NUMBER: _ClassVar[int]
    WAL_SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    LAST_PG_FLUSH_FIELD_NUMBER: _ClassVar[int]
    baseline_graph_bytes: int
    total_scenarios_bytes: int
    active_scenarios: int
    events_processed_total: int
    nodes_recomputed_total: int
    shortages_detected_total: int
    propagate_p50_us: float
    propagate_p95_us: float
    propagate_p99_us: float
    pg_writeback_queue_depth: int
    wal_size_bytes: int
    last_pg_flush: _timestamp_pb2.Timestamp
    def __init__(self, baseline_graph_bytes: _Optional[int] = ..., total_scenarios_bytes: _Optional[int] = ..., active_scenarios: _Optional[int] = ..., events_processed_total: _Optional[int] = ..., nodes_recomputed_total: _Optional[int] = ..., shortages_detected_total: _Optional[int] = ..., propagate_p50_us: _Optional[float] = ..., propagate_p95_us: _Optional[float] = ..., propagate_p99_us: _Optional[float] = ..., pg_writeback_queue_depth: _Optional[int] = ..., wal_size_bytes: _Optional[int] = ..., last_pg_flush: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
