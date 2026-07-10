"""
compare.py — SC-1: KPI comparison across 2-5 scenarios (``GET /v1/scenarios/compare``).

Read-only. No event/audit write (a pure query path, unlike promote/apply_override
which are state-changing and own their own ``events`` row). All DB access is
psycopg3; the caller (the router's ``Depends(get_db)``) owns commit/rollback —
this module never calls ``conn.commit()``/``conn.rollback()``.

KPIs per scenario (contract, architect-specified):
  (a) shortages — ``shortages`` table, ``scenario_id = :id AND status = 'active'
      AND calc_run_id = _latest_calc_run(:id)``. The ``calc_run_id`` filter is
      MANDATORY: ``shortages`` is append-only across runs (``ON CONFLICT
      (pi_node_id, calc_run_id) DO UPDATE`` — one row PER calc_run, not a single
      current row per PI), so an unscoped SUM stacks every historical run.
      ``shortage_count`` = COUNT WHERE severity_class='stockout';
      ``below_safety_stock_count`` exposed separately; ``shortage_severity_usd``
      = SUM(severity_score) — 0-honest (COUNT/SUM over zero active shortages is
      a real, meaningful 0, not a "no data" gap; severity_score is NOT NULL by
      construction in both writers of ``shortages``, see SHORTAGES_SQL).
  (b) stock_value_usd — SUM(GREATEST(closing_stock, 0) * unit_cost) over the
      scenario's ProjectedInventory nodes, averaged across the horizon's
      distinct buckets (a sum-across-time would scale with horizon length and
      mean nothing; the per-bucket total, averaged, is the "$ tied up in
      inventory" figure). unit_cost precedence MIRRORS SHORTAGES_SQL
      (engine/orchestration/propagator_sql.py:262-274) — supplier_items.unit_cost
      (preferred supplier, cheapest priced row, unit_cost > 0) then
      items.standard_cost — WITHOUT that query's final ``, 1`` unpriced-item
      proxy: this KPI is NULL-honest instead (a shortage's $ value must never
      be zero-masked by an unpriced item, matching ADR-021/#342's severity_score
      contract; a scenario-comparison $ figure has the same requirement).
      ``nodes`` (not ``inventory_snapshots``, forbidden — ADR-030 snapshots are
      baseline-only, a fork's inventory shape is invisible there) is UPDATEd
      in place by propagation (one current row per PI coordinate, no
      calc_run_id stacking), so no calc_run_id filter is needed or applied
      here (unlike shortages).
  (c) fill_rate_est = 1 - SUM(stockout shortage_qty) / SUM(PI outflows) over
      the horizon. ``nodes.outflows`` (persisted directly on ProjectedInventory
      rows by PROPAGATE_SQL / the Python kernel) IS the exploitable per-bucket
      consumption total — no need to re-derive it from Demand nodes.
      None-honest: zero (or NULL/no-PI) total demand -> None, never a masked
      1.0.

Stale (contract point 2, NO migration, NO new schema):
  stale = (a MAX(events.created_at) WHERE scenario_id=BASELINE AND
           event_type='scenario_merge' is later than the completed_at of THIS
           fork's KPI-bearing calc_run) OR (this fork's OWN most-recent
           calc_run — ANY status, a SEPARATE query from the 'completed'-only
           one used for the KPI numbers — has status='completed_stale').
  Both signals are real, already-shipped schema (migrations 002/006 for
  ``events.event_type='scenario_merge'`` — confirmed still present in the
  071 CHECK constraint list; ``calc_runs.status='completed_stale'`` —
  CalcRunManager.complete_calc_run, set when ``scenarios.baseline_snapshot_id``
  is populated). No promotion ever -> latest_merge_event_at is None -> branch
  (a) is False by construction; nothing here requires a new column.

  IMPORTANT: the OR-branch on 'completed_stale' can NEVER trigger through
  ``ScenarioManager._latest_calc_run`` (it filters ``status = 'completed'``
  ONLY, by construction — the KPI-bearing run is always literally 'completed').
  It requires a second, independent query for the scenario's TRUE latest
  calc_run row (any status) — see ``_fetch_latest_calc_run_status``.

Every entry carries ``calc_run_id``, ``computed_at`` (= that calc_run's
``completed_at``), ``stale``, ``parent_scenario_id``, ``computable``.
``comparable`` (top-level) = every entry is BOTH computable AND fresh — see
``compute_comparable`` for why a non-computable entry (``stale=None``) must
not silently pass an ``all(not stale)`` check.

Deltas are computed against ONE reference scenario per request: the baseline
sentinel if it's among the requested ids, else the first id the caller passed
(``resolve_reference_scenario_id``) — never a per-pair matrix.

Failure modes (contract point 4):
  - a malformed or unknown scenario id -> the WHOLE request is invalid
    (``ScenarioCompareError``, mapped to 422 by the router, message names the
    exact id, hand-authored, no psycopg/DSN leak).
  - a syntactically valid, EXISTING scenario with no completed calc_run
    (``ScenarioManager._latest_calc_run`` raises ``ValueError`` — reused
    verbatim, not reimplemented, matching the diff endpoint's existing catch
    at api/routers/scenarios.py) -> that ONE entry is present with
    ``kpis=None``, ``computable=False``, and a ``note`` (the ValueError's own
    hand-authored text) — the rest of the response is unaffected.
  - archived scenarios and the baseline are ordinary, valid entries (no status
    filtering here).

Deviations from the literal contract text (documented, not silent):
  - "coût ... FACTORISÉE" (point 1b): the unit-cost precedence SQL here is a
    byte-for-byte structural mirror of SHORTAGES_SQL's LATERAL (same tables,
    same columns, same ORDER BY, anchored to propagator_sql.py:262-274 by
    line-number comment above), NOT a shared Python function imported by both
    modules. ``propagator_sql.py`` is explicitly documented as byte-identical
    /parity-tested against the Python kernel (parity harness:
    scripts/parity_sql_vs_python.py) and is outside this chantier's two-file
    deliverable scope (this module + api/routers/scenarios.py); hoisting a
    shared builder would touch that file's SQL text assembly for zero behaviour
    change on its side. The mirroring is literal and comment-anchored so a
    future edit to one precedence is easy to find and sync with the other —
    this satisfies the INTENT of "never a diverging COALESCE" (leçon #347)
    without the (here, unauthorized-scope) file edit. Flagged for the
    reviewer/test-writer to decide whether a follow-up hoists both into one
    shared builder (e.g. alongside ``engine/scenario/param_overlay.py``'s
    ``resolved_field_lateral_sql``, the established precedent for this exact
    kind of factoring).
  - stock_value / fill_rate ``basis_count`` semantics were not fully pinned
    down by the contract text beyond "None-honest". This module's choice
    (documented on each pure function): stock_value's basis_count = COUNT of
    ProjectedInventory coordinates with a COMPUTED (non-NULL closing_stock)
    value — an un-computed node has no valid $ figure to contribute; fill_rate's
    basis_count = COUNT of PI buckets carrying positive outflows, which is 0 in
    exact lockstep with the "zero demand -> None" trigger by construction
    (SUM of non-negative outflows > 0 implies at least one positive row).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from typing import Optional
from uuid import UUID

from ootils_core.db.types import DictRowConnection
from ootils_core.engine.scenario.manager import ScenarioManager

logger = logging.getLogger(__name__)

# Baseline sentinel UUID (matches the seeded row in migration 002). Defined
# locally rather than imported from api/dependencies — engine/ never imports
# api/ (unidirectional import boundary); ScenarioManager (engine/scenario/
# manager.py) makes the exact same choice for the exact same reason.
BASELINE_SCENARIO_ID = UUID("00000000-0000-0000-0000-000000000001")

MIN_SCENARIO_IDS = 2
MAX_SCENARIO_IDS = 5

COST_PRECEDENCE = (
    "unit_cost = COALESCE(supplier_items.unit_cost [preferred supplier, cheapest "
    "priced row, unit_cost > 0], items.standard_cost) — NULL-honest, no "
    "fallback-to-1. Mirrors the precedence in SHORTAGES_SQL "
    "(engine/orchestration/propagator_sql.py:262-274) minus that query's "
    "unpriced-item ',1' proxy."
)


class ScenarioCompareError(Exception):
    """Hand-authored 422 payload for GET /v1/scenarios/compare.

    ``detail`` is always author-written text (a count bound violation or a
    named bad/unknown scenario id) — never a wrapped low-level exception, so
    the router can surface ``.detail`` verbatim with no DSN/psycopg leak risk
    (same carve-out shape as ``DiffError``/``ApprovalError``/``RejectionError``
    in ``staging/``)."""

    def __init__(self, detail: str) -> None:
        self.detail = detail
        super().__init__(detail)


# ---------------------------------------------------------------------------
# Result shapes — plain dataclasses (engine layer never returns Pydantic; the
# router converts). Mirrors ScenarioManager's PromoteResult/PromoteConflict.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ScenarioKpis:
    """One scenario's KPI snapshot. Every field is 0/None-honest per its own
    computing function's docstring — see module docstring (a)/(b)/(c)."""

    shortage_count: int
    below_safety_stock_count: int
    shortage_severity_usd: float
    stock_value_usd: Optional[float]
    stock_value_basis_count: int
    stock_value_unpriced_count: int
    fill_rate_est: Optional[float]
    fill_rate_basis_count: int


@dataclass(frozen=True)
class ScenarioCompareDeltas:
    """Deltas of one entry vs the request's reference scenario (entry - reference).
    ``shortage_count_delta``/``severity_usd_delta`` are always real numbers once
    both sides have ``kpis`` (those two sub-KPIs are 0-honest, never None
    themselves). ``stock_value_usd_delta``/``fill_rate_delta`` can each be
    individually None when either side's own KPI is None (unpriced inventory /
    zero demand)."""

    shortage_count_delta: int
    severity_usd_delta: float
    stock_value_usd_delta: Optional[float]
    fill_rate_delta: Optional[float]


@dataclass(frozen=True)
class ScenarioCompareEntry:
    """One requested scenario's comparison row.

    ``computable=False`` means ``kpis``/``deltas``/``stale`` are all None and
    ``note`` explains why (no completed calc_run yet)."""

    scenario_id: UUID
    name: str
    status: str
    parent_scenario_id: Optional[UUID]
    calc_run_id: Optional[UUID]
    computed_at: Optional[datetime]
    stale: Optional[bool]
    computable: bool
    note: Optional[str]
    kpis: Optional[ScenarioKpis]
    deltas: Optional[ScenarioCompareDeltas] = None


@dataclass(frozen=True)
class ScenarioCompareResult:
    entries: list[ScenarioCompareEntry]
    comparable: bool
    reference_scenario_id: UUID
    cost_precedence: str


# ---------------------------------------------------------------------------
# Pure functions — no DB, no clock, deterministic. The test-writer wave
# targets these directly (see the function list in the PR summary).
# ---------------------------------------------------------------------------


def parse_scenario_ids(raw: str) -> list[UUID]:
    """Parse the ``ids=a,b,c`` query value into UUIDs.

    Pure / DB-free. Splits on comma, strips whitespace, rejects an empty token
    and names the exact malformed token in the error (never a generic "invalid
    input"). Does not de-duplicate or bound-check count — see
    ``validate_id_count``. Order is preserved (deltas resolve the reference
    from the FIRST id, per contract point 4)."""
    tokens = [t.strip() for t in raw.split(",")]
    ids: list[UUID] = []
    for token in tokens:
        if not token:
            raise ScenarioCompareError(
                "scenarios/compare: 'ids' contains an empty scenario id — "
                "expected a comma-separated list of UUIDs, e.g. 'ids=a,b,c'."
            )
        try:
            ids.append(UUID(token))
        except ValueError:
            raise ScenarioCompareError(
                f"scenarios/compare: '{token}' is not a valid scenario id (UUID)."
            )
    return ids


def validate_id_count(scenario_ids: list[UUID]) -> None:
    """Pure bound check: MIN_SCENARIO_IDS..MAX_SCENARIO_IDS inclusive."""
    count = len(scenario_ids)
    if count < MIN_SCENARIO_IDS or count > MAX_SCENARIO_IDS:
        raise ScenarioCompareError(
            f"scenarios/compare: 'ids' must list between {MIN_SCENARIO_IDS} and "
            f"{MAX_SCENARIO_IDS} scenario ids, got {count}."
        )


def resolve_reference_scenario_id(
    scenario_ids: list[UUID],
    baseline_id: UUID = BASELINE_SCENARIO_ID,
) -> UUID:
    """Deltas compare every entry against this ONE reference: the baseline if
    it's among the requested ids, else the first id the caller passed
    (order-preserving — contract point 4: "vs le 1er id passé (ou baseline si
    présent)")."""
    return baseline_id if baseline_id in scenario_ids else scenario_ids[0]


def compute_shortage_kpis(
    shortage_count: int,
    below_safety_stock_count: int,
    shortage_severity_usd: Decimal | float,
) -> tuple[int, int, float]:
    """Type-coercion boundary for the shortages aggregate row. 0-honest, not
    None-honest: COUNT/SUM over zero active shortages for this calc_run is a
    real, meaningful 0 (a healthy scenario), not a "no data" gap — unlike
    stock_value/fill_rate, whose None triggers are about missing PRICING/DEMAND
    data, not a genuine absence of shortages."""
    return int(shortage_count), int(below_safety_stock_count), float(shortage_severity_usd)


def compute_stock_value(
    total_value: Optional[Decimal],
    bucket_count: int,
    coordinate_count: int,
    unpriced_count: int,
) -> tuple[Optional[float], int, int]:
    """stock_value_usd = SUM(GREATEST(closing_stock,0)*unit_cost) over the
    scenario's ProjectedInventory nodes, averaged across the horizon's
    distinct buckets. None-honest: zero PI coordinates with a computed value
    (``coordinate_count==0``, e.g. an un-propagated fork) -> None, never a
    masked 0.

    ``coordinate_count`` (returned unchanged) is "basis_count" per contract
    point 1b: the count of PI coordinates with a COMPUTED closing_stock — a
    node that has never been propagated (``closing_stock IS NULL``) has no
    valid $ figure to contribute and is excluded upstream by the SQL, not
    here. ``unpriced_count`` (also returned unchanged) is the subset of that
    same basis whose ``unit_cost`` resolved to NULL (COST_PRECEDENCE, no
    fallback) — those coordinates contribute $0, not a masked price."""
    coordinate_count = int(coordinate_count)
    unpriced_count = int(unpriced_count)
    bucket_count = int(bucket_count)
    if coordinate_count == 0 or bucket_count == 0:
        return None, coordinate_count, unpriced_count
    value = float(total_value) / bucket_count if total_value is not None else 0.0
    return value, coordinate_count, unpriced_count


def compute_fill_rate(
    outflows_total: Optional[Decimal],
    stockout_qty_total: Optional[Decimal],
    demand_bucket_count: int,
) -> tuple[Optional[float], int]:
    """fill_rate_est = 1 - SUM(stockout shortage_qty) / SUM(PI outflows) over
    the horizon. None-honest: zero (or NULL/no-PI) total demand -> None, NEVER
    a masked 1.0 (contract point 1c). ``demand_bucket_count`` ("basis_count")
    is forced to 0 in the None branch regardless of the caller's value — by
    construction (outflows are non-negative sums) a positive total demand
    implies at least one PI bucket with positive outflows and vice versa, so
    the two can never disagree; the explicit 0 here is defensive, not derived,
    so the None trigger and its basis_count can never diverge even on
    malformed input."""
    total_demand = float(outflows_total) if outflows_total else 0.0
    if total_demand <= 0.0:
        return None, 0
    stockout = float(stockout_qty_total) if stockout_qty_total else 0.0
    return 1.0 - (stockout / total_demand), int(demand_bucket_count)


def compute_stale(
    latest_merge_event_at: Optional[datetime],
    kpi_calc_run_completed_at: Optional[datetime],
    latest_calc_run_status: Optional[str],
) -> bool:
    """See the module docstring's "Stale" section for the full rationale.

    stale = (the baseline merged AFTER this fork's KPI-bearing calc_run
    completed) OR (this fork's OWN latest calc_run — any status — is itself
    'completed_stale'). Both branches are independent booleans; either one
    trips ``stale=True``."""
    merged_after_calc = (
        latest_merge_event_at is not None
        and kpi_calc_run_completed_at is not None
        and latest_merge_event_at > kpi_calc_run_completed_at
    )
    return bool(merged_after_calc or latest_calc_run_status == "completed_stale")


def _delta(entry_value: Optional[float], reference_value: Optional[float]) -> Optional[float]:
    if entry_value is None or reference_value is None:
        return None
    return entry_value - reference_value


def compute_deltas(
    entry_kpis: Optional[ScenarioKpis],
    reference_kpis: Optional[ScenarioKpis],
) -> Optional[ScenarioCompareDeltas]:
    """None when either side lacks ``kpis`` (a non-computable entry, or a
    non-computable reference scenario) — a delta cannot be honestly stated
    without both operands."""
    if entry_kpis is None or reference_kpis is None:
        return None
    return ScenarioCompareDeltas(
        shortage_count_delta=entry_kpis.shortage_count - reference_kpis.shortage_count,
        severity_usd_delta=entry_kpis.shortage_severity_usd - reference_kpis.shortage_severity_usd,
        stock_value_usd_delta=_delta(entry_kpis.stock_value_usd, reference_kpis.stock_value_usd),
        fill_rate_delta=_delta(entry_kpis.fill_rate_est, reference_kpis.fill_rate_est),
    )


def compute_comparable(entries: list[ScenarioCompareEntry]) -> bool:
    """True only when EVERY entry is both computable AND fresh.

    ``e.stale is False`` (not ``not e.stale``) is deliberate: a non-computable
    entry carries ``stale=None``, and ``not None`` is truthy in Python — an
    ``all(not e.stale for e in entries)`` check would silently call a set
    "comparable" even though one entry has no numbers at all. Requiring the
    literal ``False`` closes that gap without needing a separate
    ``e.computable`` guard to short-circuit it (belt AND suspenders: both are
    checked)."""
    return all(e.computable and e.stale is False for e in entries)


# ---------------------------------------------------------------------------
# DB query functions — scenario-scoped, fully parameterized, no f-string SQL.
# ---------------------------------------------------------------------------


def _load_scenario_rows(db: DictRowConnection, scenario_ids: list[UUID]) -> dict[UUID, dict]:
    rows = db.execute(
        """
        SELECT scenario_id, name, status, is_baseline, parent_scenario_id
        FROM scenarios
        WHERE scenario_id = ANY(%s)
        """,
        (list(scenario_ids),),
    ).fetchall()
    return {UUID(str(r["scenario_id"])): r for r in rows}


def _fetch_calc_run_completed_at(db: DictRowConnection, calc_run_id: UUID) -> Optional[datetime]:
    row = db.execute(
        "SELECT completed_at FROM calc_runs WHERE calc_run_id = %s",
        (calc_run_id,),
    ).fetchone()
    return row["completed_at"] if row is not None else None


def _fetch_latest_calc_run_status(db: DictRowConnection, scenario_id: UUID) -> Optional[str]:
    """The scenario's OWN most-recent calc_run row, ANY status — deliberately
    NOT filtered to ``status = 'completed'`` (unlike
    ``ScenarioManager._latest_calc_run``, which resolves the KPI-bearing run).
    A run whose status is 'completed_stale' would be invisible to that
    'completed'-only filter, so this is a second, independent query — see
    ``compute_stale``."""
    row = db.execute(
        """
        SELECT status
        FROM calc_runs
        WHERE scenario_id = %s
        ORDER BY COALESCE(completed_at, started_at, created_at) DESC
        LIMIT 1
        """,
        (scenario_id,),
    ).fetchone()
    return row["status"] if row is not None else None


def _fetch_latest_merge_event_at(db: DictRowConnection, baseline_id: UUID) -> Optional[datetime]:
    """MAX(created_at) of every 'scenario_merge' event ever written on the
    baseline (ScenarioManager.promote — always ``scenario_id=_BASELINE_ID``,
    confirmed against migrations 006/071's CHECK list and manager.py's
    promote()). None when the baseline has never been merged into."""
    row = db.execute(
        """
        SELECT MAX(created_at) AS latest_merge_at
        FROM events
        WHERE scenario_id = %s AND event_type = 'scenario_merge'
        """,
        (baseline_id,),
    ).fetchone()
    return row["latest_merge_at"] if row is not None else None


def _fetch_shortage_kpis(
    db: DictRowConnection, scenario_id: UUID, calc_run_id: UUID
) -> dict:
    """calc_run_id filter is MANDATORY — see the module docstring (a)."""
    row = db.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE severity_class = 'stockout') AS shortage_count,
            COUNT(*) FILTER (WHERE severity_class = 'below_safety_stock')
                AS below_safety_stock_count,
            COALESCE(SUM(severity_score), 0) AS shortage_severity_usd,
            COALESCE(SUM(shortage_qty) FILTER (WHERE severity_class = 'stockout'), 0)
                AS stockout_qty_total
        FROM shortages
        WHERE scenario_id = %s AND status = 'active' AND calc_run_id = %s
        """,
        (scenario_id, calc_run_id),
    ).fetchone()
    if row is None:  # pragma: no cover — an aggregate always returns one row
        return {
            "shortage_count": 0,
            "below_safety_stock_count": 0,
            "shortage_severity_usd": Decimal("0"),
            "stockout_qty_total": Decimal("0"),
        }
    return dict(row)


def _fetch_stock_value_kpis(db: DictRowConnection, scenario_id: UUID) -> dict:
    """unit_cost precedence — see COST_PRECEDENCE / the module docstring's
    "Deviations" section for why this mirrors, rather than imports,
    SHORTAGES_SQL's LATERAL (propagator_sql.py:262-274)."""
    row = db.execute(
        """
        WITH pi_priced AS (
            SELECT
                pi.bucket_sequence,
                GREATEST(pi.closing_stock, 0) AS pos_stock,
                COALESCE(sup.unit_cost, i.standard_cost) AS unit_cost
            FROM nodes pi
            LEFT JOIN items i ON i.item_id = pi.item_id
            LEFT JOIN LATERAL (
                SELECT unit_cost
                FROM supplier_items
                WHERE item_id = pi.item_id
                  AND unit_cost IS NOT NULL AND unit_cost > 0
                ORDER BY is_preferred DESC, unit_cost ASC
                LIMIT 1
            ) sup ON TRUE
            WHERE pi.scenario_id = %s
              AND pi.node_type = 'ProjectedInventory'
              AND pi.active = TRUE
              AND pi.closing_stock IS NOT NULL
        )
        SELECT
            COUNT(*) AS coordinate_count,
            COUNT(*) FILTER (WHERE unit_cost IS NULL) AS unpriced_count,
            COUNT(DISTINCT bucket_sequence) AS bucket_count,
            SUM(pos_stock * COALESCE(unit_cost, 0)) AS total_value
        FROM pi_priced
        """,
        (scenario_id,),
    ).fetchone()
    if row is None:  # pragma: no cover — an aggregate always returns one row
        return {
            "coordinate_count": 0,
            "unpriced_count": 0,
            "bucket_count": 0,
            "total_value": None,
        }
    return dict(row)


def _fetch_fill_rate_denominator(db: DictRowConnection, scenario_id: UUID) -> dict:
    """No calc_run_id filter — ``nodes`` is UPDATEd in place (one current row
    per PI coordinate), unlike the append-only ``shortages`` table."""
    row = db.execute(
        """
        SELECT
            COALESCE(SUM(outflows), 0) AS outflows_total,
            COUNT(*) FILTER (WHERE outflows > 0) AS demand_bucket_count
        FROM nodes
        WHERE scenario_id = %s AND node_type = 'ProjectedInventory' AND active = TRUE
        """,
        (scenario_id,),
    ).fetchone()
    if row is None:  # pragma: no cover — an aggregate always returns one row
        return {"outflows_total": Decimal("0"), "demand_bucket_count": 0}
    return dict(row)


# ---------------------------------------------------------------------------
# Orchestration — one entry, then the whole request.
# ---------------------------------------------------------------------------


def _build_entry(
    db: DictRowConnection, scenario_id: UUID, scenario_row: dict
) -> ScenarioCompareEntry:
    parent_scenario_id = (
        UUID(str(scenario_row["parent_scenario_id"]))
        if scenario_row.get("parent_scenario_id")
        else None
    )

    manager = ScenarioManager()
    try:
        # The canonical resolver (contract point 1a: "calc_run_id=_latest_calc_run
        # (:id) — le filtre calc_run_id est OBLIGATOIRE"), reused verbatim — same
        # pattern already used by the diff endpoint (api/routers/scenarios.py).
        calc_run_id = manager._latest_calc_run(scenario_id, db)  # noqa: SLF001
    except ValueError as exc:
        return ScenarioCompareEntry(
            scenario_id=scenario_id,
            name=scenario_row["name"],
            status=scenario_row["status"],
            parent_scenario_id=parent_scenario_id,
            calc_run_id=None,
            computed_at=None,
            stale=None,
            computable=False,
            note=str(exc),
            kpis=None,
        )

    completed_at = _fetch_calc_run_completed_at(db, calc_run_id)
    latest_status = _fetch_latest_calc_run_status(db, scenario_id)
    latest_merge_at = _fetch_latest_merge_event_at(db, BASELINE_SCENARIO_ID)
    stale = compute_stale(latest_merge_at, completed_at, latest_status)

    shortage_row = _fetch_shortage_kpis(db, scenario_id, calc_run_id)
    shortage_count, below_safety_stock_count, shortage_severity_usd = compute_shortage_kpis(
        shortage_row["shortage_count"],
        shortage_row["below_safety_stock_count"],
        shortage_row["shortage_severity_usd"],
    )

    stock_row = _fetch_stock_value_kpis(db, scenario_id)
    stock_value_usd, stock_value_basis_count, stock_value_unpriced_count = compute_stock_value(
        stock_row["total_value"],
        stock_row["bucket_count"],
        stock_row["coordinate_count"],
        stock_row["unpriced_count"],
    )

    fill_row = _fetch_fill_rate_denominator(db, scenario_id)
    fill_rate_est, fill_rate_basis_count = compute_fill_rate(
        fill_row["outflows_total"],
        shortage_row["stockout_qty_total"],
        fill_row["demand_bucket_count"],
    )

    kpis = ScenarioKpis(
        shortage_count=shortage_count,
        below_safety_stock_count=below_safety_stock_count,
        shortage_severity_usd=shortage_severity_usd,
        stock_value_usd=stock_value_usd,
        stock_value_basis_count=stock_value_basis_count,
        stock_value_unpriced_count=stock_value_unpriced_count,
        fill_rate_est=fill_rate_est,
        fill_rate_basis_count=fill_rate_basis_count,
    )

    return ScenarioCompareEntry(
        scenario_id=scenario_id,
        name=scenario_row["name"],
        status=scenario_row["status"],
        parent_scenario_id=parent_scenario_id,
        calc_run_id=calc_run_id,
        computed_at=completed_at,
        stale=stale,
        computable=True,
        note=None,
        kpis=kpis,
    )


def compare_scenarios(db: DictRowConnection, scenario_ids: list[UUID]) -> ScenarioCompareResult:
    """Build the full comparison payload for an already bounds-checked list of
    scenario ids (caller must have run ``validate_id_count`` first — this
    function only re-validates EXISTENCE, a DB-dependent check that
    ``validate_id_count`` cannot make).

    Raises ``ScenarioCompareError`` (422, id named) for any id that is not an
    existing row in ``scenarios``. A syntactically valid, existing scenario
    with no completed calc_run does NOT raise — it gets a
    ``computable=False`` entry (see ``_build_entry``)."""
    scenario_rows = _load_scenario_rows(db, scenario_ids)
    for sid in scenario_ids:
        if sid not in scenario_rows:
            raise ScenarioCompareError(f"scenarios/compare: unknown scenario id '{sid}'.")

    entries = [_build_entry(db, sid, scenario_rows[sid]) for sid in scenario_ids]

    reference_id = resolve_reference_scenario_id(scenario_ids)
    reference_kpis = next(e.kpis for e in entries if e.scenario_id == reference_id)
    entries = [replace(e, deltas=compute_deltas(e.kpis, reference_kpis)) for e in entries]

    comparable = compute_comparable(entries)

    logger.info(
        "scenario.compare ids=%s reference=%s comparable=%s",
        [str(i) for i in scenario_ids],
        reference_id,
        comparable,
    )

    return ScenarioCompareResult(
        entries=entries,
        comparable=comparable,
        reference_scenario_id=reference_id,
        cost_precedence=COST_PRECEDENCE,
    )
