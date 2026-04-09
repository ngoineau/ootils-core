"""
Domain models for the Ootils planning engine.
These are pure Python dataclasses — no ORM, no DB coupling.
All field types are explicit (no JSONB blobs).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import List, Optional
from uuid import UUID, uuid4


# ---------------------------------------------------------------------------
# Core domain entities
# ---------------------------------------------------------------------------


@dataclass
class Scenario:
    scenario_id: UUID
    name: str
    description: Optional[str] = None
    parent_scenario_id: Optional[UUID] = None
    is_baseline: bool = False
    baseline_snapshot_id: Optional[UUID] = None
    status: str = "active"
    as_of_date: Optional[date] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # Sentinel baseline UUID (matches the seeded row in migration)
    BASELINE_ID: UUID = UUID("00000000-0000-0000-0000-000000000001")


@dataclass
class Item:
    item_id: UUID
    name: str
    item_type: str = "finished_good"
    uom: str = "EA"
    status: str = "active"
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class Location:
    location_id: UUID
    name: str
    location_type: str = "dc"
    country: Optional[str] = None
    timezone: Optional[str] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class Node:
    node_id: UUID
    node_type: str
    scenario_id: UUID
    item_id: Optional[UUID] = None
    location_id: Optional[UUID] = None

    # Quantity
    quantity: Optional[Decimal] = None
    qty_uom: Optional[str] = None

    # Temporal fields
    time_grain: Optional[str] = None
    time_ref: Optional[date] = None
    time_span_start: Optional[date] = None
    time_span_end: Optional[date] = None

    # Engine state
    is_dirty: bool = False
    last_calc_run_id: Optional[UUID] = None
    active: bool = True

    # PI-specific
    projection_series_id: Optional[UUID] = None
    bucket_sequence: Optional[int] = None

    # PI computation results
    opening_stock: Optional[Decimal] = None
    inflows: Optional[Decimal] = None
    outflows: Optional[Decimal] = None
    closing_stock: Optional[Decimal] = None
    has_shortage: bool = False
    shortage_qty: Decimal = Decimal("0")

    # Grain mix tracking
    has_exact_date_inputs: bool = False
    has_week_inputs: bool = False
    has_month_inputs: bool = False

    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


# Typed node subclasses for clarity in type hints
ProjectedInventoryNode = Node  # node_type == 'ProjectedInventory'
PurchaseOrderNode = Node       # node_type == 'PurchaseOrderSupply'
OnHandNode = Node              # node_type == 'OnHandSupply'


@dataclass
class Edge:
    edge_id: UUID
    edge_type: str
    from_node_id: UUID
    to_node_id: UUID
    scenario_id: UUID
    priority: int = 0
    weight_ratio: Decimal = Decimal("1.0")
    effective_start: Optional[date] = None
    effective_end: Optional[date] = None
    active: bool = True
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class ProjectionSeries:
    series_id: UUID
    item_id: UUID
    location_id: UUID
    scenario_id: UUID
    horizon_start: date
    horizon_end: date
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class NodeTypeTemporalPolicy:
    policy_id: UUID
    node_type: str
    zone1_grain: str = "day"
    zone1_end_days: int = 90
    zone2_grain: str = "week"
    zone2_end_days: int = 180
    zone3_grain: str = "month"
    week_start_dow: int = 0  # 0 = Monday
    active: bool = True
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class CalcRun:
    calc_run_id: UUID
    scenario_id: UUID
    triggered_by_event_ids: list[UUID] = field(default_factory=list)
    is_full_recompute: bool = False
    dirty_node_count: Optional[int] = None
    nodes_recalculated: int = 0
    nodes_unchanged: int = 0
    status: str = "pending"  # pending | running | completed | completed_stale | failed
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class PlanningEvent:
    event_id: UUID
    event_type: str
    scenario_id: UUID
    trigger_node_id: Optional[UUID] = None
    field_changed: Optional[str] = None
    old_date: Optional[date] = None
    new_date: Optional[date] = None
    old_quantity: Optional[Decimal] = None
    new_quantity: Optional[Decimal] = None
    old_text: Optional[str] = None
    new_text: Optional[str] = None
    processed: bool = False
    processed_at: Optional[datetime] = None
    source: str = "api"
    user_ref: Optional[str] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


# ---------------------------------------------------------------------------
# Projection kernel DTOs
# ---------------------------------------------------------------------------


@dataclass
class SupplyEvent:
    """A supply contribution (PO, WO, Transfer) relevant to a PI bucket."""
    node_id: UUID
    node_type: str
    quantity: Decimal
    time_ref: date          # exact date of supply
    time_grain: str = "exact_date"


@dataclass
class DemandEvent:
    """A demand contribution (forecast, customer order) relevant to a PI bucket."""
    node_id: UUID
    node_type: str
    quantity: Decimal
    time_ref: Optional[date] = None
    time_span_start: Optional[date] = None
    time_span_end: Optional[date] = None
    time_grain: str = "exact_date"


@dataclass
class ProjectedInventoryResult:
    """Output of ProjectionKernel.compute_pi_node — pure computation result."""
    opening_stock: Decimal
    inflows: Decimal
    outflows: Decimal
    closing_stock: Decimal
    has_shortage: bool
    shortage_qty: Decimal


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class CycleDetectedError(Exception):
    """Raised when inserting an edge would create a cycle in the planning graph."""
    def __init__(self, from_id: UUID, to_id: UUID, scenario_id: UUID):
        self.from_id = from_id
        self.to_id = to_id
        self.scenario_id = scenario_id
        super().__init__(
            f"Adding edge {from_id} → {to_id} in scenario {scenario_id} would create a cycle"
        )


class EngineStartupError(Exception):
    """Raised by startup_cycle_check if the graph has a cycle."""
    pass


# ---------------------------------------------------------------------------
# Explainability models (Sprint M3)
# ---------------------------------------------------------------------------


@dataclass
class CausalStep:
    """One step in a causal chain explaining a planning result."""
    step: int
    node_id: Optional[UUID]
    node_type: Optional[str]
    edge_type: Optional[str]
    fact: str  # human-readable description of this step


@dataclass
class Explanation:
    """Structured, traversable causal explanation for a planning result node."""
    explanation_id: UUID
    calc_run_id: UUID
    target_node_id: UUID
    target_type: str          # e.g. 'Shortage', 'ProjectedInventory'
    root_cause_node_id: Optional[UUID]
    causal_path: list[CausalStep]
    summary: str              # 1-line plain English
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


# ---------------------------------------------------------------------------
# Shortage Detection models (Sprint M4)
# ---------------------------------------------------------------------------


@dataclass
class ShortageRecord:
    """A detected inventory shortage on a ProjectedInventory node."""
    shortage_id: UUID
    scenario_id: UUID
    pi_node_id: UUID        # the PI node with closing_stock < 0
    item_id: Optional[UUID]
    location_id: Optional[UUID]
    shortage_date: date     # time_span_start of the PI node
    shortage_qty: Decimal   # abs(closing_stock)
    severity_score: Decimal  # qty × days_at_shortage × unit_cost_proxy
    explanation_id: Optional[UUID]  # FK to explanations (M3)
    calc_run_id: UUID
    status: str = "active"  # active | resolved
    severity_class: Optional[str] = None  # 'stockout' | 'below_safety_stock'
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


# ---------------------------------------------------------------------------
# Allocation Engine results
# ---------------------------------------------------------------------------


@dataclass
class AllocationResult:
    """Summary of a completed allocation pass."""
    scenario_id: UUID
    demands_total: int
    demands_fully_allocated: int
    demands_partially_allocated: int
    demands_unallocated: int
    total_qty_demanded: Decimal
    total_qty_allocated: Decimal
    edges_created: int
    edges_updated: int
    run_at: datetime


# ---------------------------------------------------------------------------
# Scenario M5 models
# ---------------------------------------------------------------------------


@dataclass
class ScenarioOverride:
    """
    A user/agent override applied to a specific node field within a scenario.
    Values are serialized as TEXT — this is intentional: overrides represent
    user intent, not computed state.
    """
    override_id: UUID
    scenario_id: UUID
    node_id: UUID
    field_name: str
    old_value: Optional[str]
    new_value: str
    applied_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    applied_by: Optional[str] = None


@dataclass
class ScenarioDiff:
    """
    One field-level difference between a baseline calc_run result and a
    scenario calc_run result on the same node.
    """
    diff_id: UUID
    scenario_id: UUID
    baseline_calc_run_id: UUID
    scenario_calc_run_id: UUID
    node_id: UUID
    field_name: str
    baseline_value: Optional[str]
    scenario_value: Optional[str]
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


# ---------------------------------------------------------------------------
# AI Agent models (Sprint M7)
# ---------------------------------------------------------------------------


@dataclass
class AgentRecommendation:
    """A single actionable recommendation produced by the autonomous agent."""
    issue_node_id: UUID
    root_cause_summary: str
    action_type: str          # 'expedite_supply' | 'reduce_demand' | 'no_action' | 'escalate'
    action_detail: str
    simulation_scenario_id: Optional[UUID]
    confidence: str           # 'high' | 'medium' | 'low'


@dataclass
class AgentReport:
    """Full report produced by a single OotilsAgent.run() execution."""
    issues_found: int
    issues_analyzed: int
    simulations_run: int
    recommendations: List[AgentRecommendation]
    run_at: datetime
    summary: str              # 1-paragraph plain English
