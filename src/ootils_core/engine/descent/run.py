"""
run.py — the demand-descent run (DESC-1 PR-B, ADR-043 §1).

``execute_descent`` is "the link" ADR-043 describes: it reads national
(pooled, virtual-channel) demand + the scenario-resolved split percentages +
item->DC eligibility, and MATERIALIZES ordinary per-DC ``ForecastDemand``/
``CustomerOrderDemand`` nodes on the real distribution centers, deactivating
the national source node it split (anti-double-count). No new ``node_type``,
no read-path touched (ADR-043 §"Alternatives rejetées") — the DRP/MRP
loaders, the projection kernel and the shortage detector already understand
these two node types on any location.

WHAT THIS RUN DOES NOT DO (by design, per the plan):
  * It never recomputes the projection. The derived nodes are wired to their
    DC's PI buckets (``graph_wiring.ensure_projection_series`` /
    ``wire_node_to_pi``, the SAME helpers ``api/routers/ingest.py`` uses —
    see that module's docstring for why this is a single shared
    implementation, not a second writer) so the GRAPH STRUCTURE is correct
    immediately, but the PI buckets' numeric ``opening_stock``/``inflows``/
    ``outflows``/``closing_stock`` stay whatever they were before this run
    until a SEPARATE recompute call (``POST /v1/calc/run`` with
    ``full_recompute=true``, or an incremental ``POST /v1/events``) actually
    walks the propagation kernel. This split is intentional (the plan calls
    it out explicitly): descent is a DATA MATERIALIZATION step, propagation
    is a distinct concern with its own advisory-lock/dirty-flag lifecycle
    (``engine/orchestration/propagator.py``) that this run does not touch.
  * It never computes split percentages. ``demand_split_pct`` is read
    as-is — whoever populated it (a future calibration job built on
    ``engine/descent/shares.py``, or a hand-seeded cold-start row) owns that
    computation. This run's ONLY responsibility is applying whatever
    percentages already exist, honestly reporting what it could not apply
    (``items_without_shares``) rather than inventing a split.
  * It never commits or rolls back. The caller (``get_db`` in the router, a
    future CLI's own connection) owns the transaction — same contract as
    every other engine-layer writer in this repo (``ScenarioManager``,
    ``engine/maintenance/purge.py``).

SCENARIO RESOLUTION (ADR-025 COALESCE pattern, mirrored not reused —
``demand_split_pct`` has exactly ONE reader, this run, so a full
``resolved_params_sql()``-style fanned-out resolver would be over- engineering;
see ADR-043 §2): for each (item_id, dc_location_id), the scenario's OWN row
wins if one exists, else the baseline row (``scenario_id IS NULL``) applies —
``SELECT DISTINCT ON (item_id, dc_location_id) ... ORDER BY ...,
(scenario_id IS NULL) ASC`` picks the non-NULL (fork-scoped) row first when
both exist. V1 is annual-only (``season_bucket IS NULL``, migration 083's own
V1 scope) — a seasonal descent is future work.

ELIGIBILITY GATE (``item_dc_eligibility``, read as a defensive cross-check
against ``demand_split_pct``, not a redundant recomputation of it): a (item,
dc) pair is kept ONLY if ``item_dc_eligibility`` carries an explicit
``eligible=TRUE`` row for it — an ABSENT pair is treated as NOT eligible,
the SAME "never invent eligibility" convention
``engine/descent/shares.py``'s own ``DcEligibility``/``_build_eligibility_map``
already establishes (deliberately mirrored, not diverged from, even though
this module does not import that private helper — see "RESIDUAL IMPUTATION"
below for why). This is not the over-cautious gate it might first appear:
whatever process populates ``demand_split_pct`` in the intended flow (a
calibration job built on ``compute_split_shares``/``equal_split_shares``,
which themselves REQUIRE eligibility as an input and never emit a share for
an ineligible/absent DC) can only ever produce a row already backed by a
positive eligibility row — this gate is therefore a correctness check that
should be a no-op in the normal flow, catching a stale/hand-inserted
``demand_split_pct`` row whose eligibility was later revoked (or a manual
entry missing its companion eligibility row) rather than a routine cold-start
obstacle. Every exclusion (explicit ``eligible=FALSE`` or plain absence) is
logged (``descent.eligibility_excluded``) for visibility, and the item's
remaining shares (if any) are renormalized — see "RESIDUAL IMPUTATION" below.

RESIDUAL IMPUTATION — TWO STAGES, both mirroring the SAME rule
``engine/descent/shares.py::_normalize_with_residual`` already uses (impute
the WHOLE residual onto the single largest share, ties broken by the
smallest ``dc_location_id``, deterministic and auditable — never smeared
evenly). Reimplemented locally rather than importing the private helper
(different quantum, different domain: percentages there, quantities here;
see ``_normalize_pct``/``_impute_qty_residual`` below), same principle:
  1. PCT stage (``_normalize_pct``): whatever raw percentages resolve for an
     item (after the eligibility filter above) are renormalized to sum to
     EXACTLY 1 at 8 decimal places (matching ``demand_split_pct.pct``'s
     ``NUMERIC(9,8)``). This is a defensive guarantee, not just a rounding
     nicety: a fork that overrides ONLY SOME of an item's DC rows (leaving
     the rest to fall back to baseline per-row) can resolve a raw set that
     does not itself sum to 1 — renormalizing here is what makes the
     "derived total == source total" invariant hold regardless of how the
     fork/baseline COALESCE landed. ``demand_descent_lines.pct_applied`` is
     explicitly documented (migration 083) to be allowed to diverge from the
     live ``demand_split_pct.pct`` — this is the SAME kind of divergence,
     just triggered by renormalization instead of a later edit; the ledger
     freezes what was actually applied either way.
  2. QUANTITY stage (``_impute_qty_residual``): each DC's derived quantity is
     ``qty_source * pct_applied`` quantized to 6 decimal places (matching
     ``demand_descent_lines.qty_source``/``qty_derived``'s ``NUMERIC(18,6)``).
     Quantizing N shares independently can leave a few-unit-in-the-6th-decimal
     rounding gap between ``qty_source`` and ``SUM(qty_derived)``; that gap is
     imputed, in full, onto the largest-``pct_applied`` DC. This guarantees
     ``SUM(qty_derived) == qty_source`` EXACTLY for every split source node —
     the pooled national total (Truth B, ADR-021/ADR-043 §"Convergence") is
     preserved to the last unit, only its carrying nodes change.

INVARIANT #455 (bulk INSERT + same-transaction reread of stale stats):
evaluated and applied defensively. This run's own write loop inserts a
demand node THEN immediately calls ``wire_node_to_pi``, which SELECTs
``nodes``/``edges`` back on the SAME connection/transaction, per derived
line — structurally similar to (though smaller-scale than) the
``dirty_nodes`` -> ``PROPAGATE_SQL`` pattern #455 was written for. A single
demand-descent run can plausibly create thousands of derived nodes across
many (item, DC) pairs in ONE transaction (unlike a single ``/v1/ingest/*``
call, which processes a much smaller batch and has never needed this
guard). ``ANALYZE nodes`` is therefore run ONCE at the end of the write
phase (mirroring ``engine/maintenance/purge.py``'s ``_analyze_tables``:
once, in bulk, not per-row) whenever at least one line was written — cheap
relative to the bulk insert, and closes the risk category defensively even
though this run's own per-node ``wire_node_to_pi`` queries are individually
small, targeted, index-backed lookups (not the large aggregate scan #455's
original bug depended on).

IDEMPOTENCE: a re-run over the SAME scenario is a clean no-op once a source
node has been split — the national-demand SELECT is scoped to
``active = TRUE``, and this run deactivates every source it splits in the
SAME transaction, so a second call simply finds nothing left to process
(zero source nodes considered, zero lines written, no calc_run/event
created). The derived ``node_id`` is ALSO deterministic
(``uuid5(_DESCENT_NODE_NAMESPACE, f"{source_node_id}:{dc_location_id}")``,
``ON CONFLICT (node_id) DO NOTHING``) as a second, independent idempotence
guard for the rarer case of a source node being manually reactivated and
re-split — the node itself is never duplicated even then, though a fresh
``demand_descent_lines`` row IS written for that new run (the ledger is a
per-RUN audit trail, not itself deduplicated across runs).

Never commits, never rolls back — the caller owns the transaction.
"""
from __future__ import annotations

import logging
import uuid as _uuid_mod
from dataclasses import dataclass
from datetime import date as _date
from datetime import datetime
from decimal import Decimal
from typing import Optional

from ootils_core.constants import BASELINE_SCENARIO_ID
from ootils_core.db.types import DictRowConnection
from ootils_core.engine.events.emit import emit_stream_event
from ootils_core.engine.graph_wiring import ensure_projection_series, wire_node_to_pi

logger = logging.getLogger(__name__)

# The two demand node types in scope (ADR-043 pilot decision 4: "Périmètre =
# prévisions + commandes" — DependentDemand/TransferDemand are already
# rattached to real sites by construction and are out of scope by design).
_DEMAND_NODE_TYPES: tuple[str, ...] = ("CustomerOrderDemand", "ForecastDemand")

# Fixed namespace for the derived node_id's uuid5 — a pure hash of a constant
# string, so this value is stable across processes/imports without needing to
# hand-mint and paste a literal UUID. NEVER change this string once shipped:
# doing so would change every future derived node_id and break the
# ON CONFLICT DO NOTHING idempotence guarantee for re-runs against nodes
# created under the old namespace.
_DESCENT_NODE_NAMESPACE: _uuid_mod.UUID = _uuid_mod.uuid5(
    _uuid_mod.NAMESPACE_URL, "https://ootils-core/engine/descent/derived-node"
)

# NUMERIC(9,8) — matches demand_split_pct.pct / demand_descent_lines.pct_applied.
_PCT_QUANTUM = Decimal("0.00000001")
# NUMERIC(18,6) — matches demand_descent_lines.qty_source/qty_derived (the
# canonical scaled quantity precision, see migration 083's header TYPE NOTES).
_QTY_QUANTUM = Decimal("0.000001")
_ZERO = Decimal("0")
_ONE = Decimal("1")


class DescentError(ValueError):
    """Raised for a structural precondition the run refuses to paper over
    (an unknown ``scenario_id``). Never raised for "this item has no usable
    share" — that is traced in ``DescentResult.items_without_shares``
    instead (ADR-043's fail-loudly, never-invent rule: the demand simply
    stays national)."""


@dataclass(frozen=True)
class DescentResult:
    """The outcome of one ``execute_descent`` call.

    ``descent_run_id``/``event_id`` are ``None`` on ``dry_run=True`` (nothing
    written) AND on a real run that found zero lines to write (nothing to
    announce — mirrors ``POST /v1/snapshots``' "skip the event on an empty
    capture" convention). ``items_without_shares`` is always populated
    (dry_run or not) — the fail-loudly trace of which items' national demand
    stayed national because no usable split existed for them.
    """

    scenario_id: _uuid_mod.UUID
    dry_run: bool
    descent_run_id: Optional[_uuid_mod.UUID]
    event_id: Optional[_uuid_mod.UUID]
    source_nodes_considered: int
    source_nodes_deactivated: int
    derived_nodes_created: int
    lines_written: int
    items_without_shares: tuple[_uuid_mod.UUID, ...]


@dataclass(frozen=True)
class _SourceNode:
    """One national demand node, plus everything needed to derive its
    per-DC children (a readable label, its own temporal fields verbatim)."""

    node_id: _uuid_mod.UUID
    node_type: str
    item_id: _uuid_mod.UUID
    location_id: _uuid_mod.UUID
    quantity: Decimal
    qty_uom: Optional[str]
    time_grain: Optional[str]
    time_ref: _date
    time_span_start: Optional[_date]
    time_span_end: Optional[_date]
    label: str


@dataclass(frozen=True)
class _PlannedLine:
    """One (source, DC) split — fully computed, before any write happens.
    ``derived_external_id`` is filled in a second pass, once every needed
    DC's own ``external_id`` is known (see ``execute_descent``)."""

    source: _SourceNode
    dc_location_id: _uuid_mod.UUID
    pct_applied: Decimal
    qty_source: Decimal
    qty_derived: Decimal
    derived_node_id: _uuid_mod.UUID
    derived_external_id: str


def _resolve_source_label(row: dict) -> str:
    """Build a human-READABLE label for a source node — never a bare UUID
    when a real business identifier is available.

    Precedence: the node's own ``external_id`` (rare — populated today only
    for ``Resource`` nodes, but the column is generic) > the CustomerOrder's
    ERP business key (``external_references``, entity_type='customer_order'
    — the genuine readable identifier CO nodes carry) > a label built from
    the item's own ``external_id`` + a type tag + the demand's anchor date
    (ForecastDemand nodes carry NO natural external identifier anywhere in
    the schema — this is the best available readable fallback, built from
    real business keys rather than a random node_id) > the raw node_id as a
    last resort (only reachable if even the item has no external_id, which
    should not happen in practice).
    """
    node_external_id = row.get("node_external_id")
    if node_external_id:
        return str(node_external_id)
    co_external_id = row.get("co_external_id")
    if co_external_id:
        return str(co_external_id)
    item_external_id = row.get("item_external_id")
    if item_external_id:
        tag = "CO" if row["node_type"] == "CustomerOrderDemand" else "FD"
        time_ref = row.get("time_ref")
        if time_ref is not None:
            return f"{item_external_id}-{tag}-{time_ref.isoformat()}"
        return f"{item_external_id}-{tag}"
    return str(row["node_id"])


def _fetch_national_demand(
    conn: DictRowConnection, scenario_id: _uuid_mod.UUID
) -> list[_SourceNode]:
    """SELECT-only: active CustomerOrderDemand/ForecastDemand nodes on
    non-stocking (``locations.is_stocking = FALSE``) locations for
    ``scenario_id`` — the national/pooled demand this run splits.
    Deterministic order (``ORDER BY n.node_id``)."""
    rows = conn.execute(
        """
        SELECT
            n.node_id, n.node_type, n.item_id, n.location_id,
            n.quantity, n.qty_uom, n.time_grain, n.time_ref,
            n.time_span_start, n.time_span_end,
            n.external_id AS node_external_id,
            i.external_id AS item_external_id,
            er.external_id AS co_external_id
        FROM nodes n
        JOIN locations l ON l.location_id = n.location_id
        LEFT JOIN items i ON i.item_id = n.item_id
        LEFT JOIN external_references er
            ON er.entity_type = 'customer_order' AND er.internal_id = n.node_id
        WHERE n.scenario_id = %s
          AND n.active = TRUE
          AND n.node_type = ANY(%s)
          AND l.is_stocking = FALSE
        ORDER BY n.node_id
        """,
        (scenario_id, list(_DEMAND_NODE_TYPES)),
    ).fetchall()

    sources: list[_SourceNode] = []
    for row in rows:
        quantity = row["quantity"]
        if quantity is None:
            raise DescentError(
                f"national demand node {row['node_id']} has a NULL quantity — "
                "a demand node without a quantity cannot be split (fail-loudly: "
                "never treated as an implicit zero)."
            )
        time_ref = row["time_ref"]
        if time_ref is None:
            raise DescentError(
                f"national demand node {row['node_id']} has a NULL time_ref — "
                "a demand node without an anchor date cannot be wired to a PI "
                "bucket (fail-loudly: every CustomerOrderDemand/ForecastDemand "
                "node the ingest paths write always carries one; a NULL here "
                "signals corrupt/hand-inserted data)."
            )
        sources.append(
            _SourceNode(
                node_id=_uuid_mod.UUID(str(row["node_id"])),
                node_type=row["node_type"],
                item_id=_uuid_mod.UUID(str(row["item_id"])),
                location_id=_uuid_mod.UUID(str(row["location_id"])),
                quantity=quantity,
                qty_uom=row["qty_uom"],
                time_grain=row["time_grain"],
                time_ref=time_ref,
                time_span_start=row["time_span_start"],
                time_span_end=row["time_span_end"],
                label=_resolve_source_label(row),
            )
        )
    return sources


def _fetch_resolved_shares(
    conn: DictRowConnection,
    scenario_id: _uuid_mod.UUID,
    item_ids: list[_uuid_mod.UUID],
) -> dict[_uuid_mod.UUID, dict[_uuid_mod.UUID, Decimal]]:
    """Resolve ``demand_split_pct`` per (item_id, dc_location_id), scenario
    row winning over the baseline (``scenario_id IS NULL``) row per key —
    the ADR-025-mirrored COALESCE precedence (see module docstring). V1
    annual-only: ``season_bucket IS NULL``. RAW resolution — the eligibility
    gate and Sigma=1 renormalization happen in the caller, not here."""
    if not item_ids:
        return {}
    rows = conn.execute(
        """
        SELECT DISTINCT ON (item_id, dc_location_id)
            item_id, dc_location_id, pct
        FROM demand_split_pct
        WHERE item_id = ANY(%s)
          AND (scenario_id = %s OR scenario_id IS NULL)
          AND season_bucket IS NULL
        ORDER BY item_id, dc_location_id, (scenario_id IS NULL) ASC
        """,
        (item_ids, scenario_id),
    ).fetchall()

    shares: dict[_uuid_mod.UUID, dict[_uuid_mod.UUID, Decimal]] = {}
    for row in rows:
        item_id = _uuid_mod.UUID(str(row["item_id"]))
        dc_id = _uuid_mod.UUID(str(row["dc_location_id"]))
        shares.setdefault(item_id, {})[dc_id] = row["pct"]
    return shares


def _fetch_eligibility(
    conn: DictRowConnection, item_ids: list[_uuid_mod.UUID]
) -> dict[tuple[_uuid_mod.UUID, _uuid_mod.UUID], bool]:
    """(item_id, dc_location_id) -> eligible, for every row that exists. A
    pair absent from the return value is treated by the caller as NOT
    eligible (see module docstring's "ELIGIBILITY GATE" section)."""
    if not item_ids:
        return {}
    rows = conn.execute(
        """
        SELECT item_id, dc_location_id, eligible
        FROM item_dc_eligibility
        WHERE item_id = ANY(%s)
        """,
        (item_ids,),
    ).fetchall()
    return {
        (_uuid_mod.UUID(str(row["item_id"])), _uuid_mod.UUID(str(row["dc_location_id"]))): bool(
            row["eligible"]
        )
        for row in rows
    }


def _fetch_dc_external_ids(
    conn: DictRowConnection, dc_location_ids: set[_uuid_mod.UUID]
) -> dict[_uuid_mod.UUID, str]:
    """location_id -> external_id (the readable DC code, e.g. PAT/DCW/DAL),
    for the DCs this run actually derives demand onto."""
    if not dc_location_ids:
        return {}
    rows = conn.execute(
        "SELECT location_id, external_id FROM locations WHERE location_id = ANY(%s)",
        (list(dc_location_ids),),
    ).fetchall()
    return {
        _uuid_mod.UUID(str(row["location_id"])): (row["external_id"] or str(row["location_id"]))
        for row in rows
    }


def _normalize_pct(weights: dict[_uuid_mod.UUID, Decimal]) -> dict[_uuid_mod.UUID, Decimal]:
    """Renormalize positive weights to sum to EXACTLY 1 at 8 decimal places.

    Mirrors ``engine/descent/shares.py::_normalize_with_residual`` (same
    rule, reimplemented for this module's quantum/domain — see the module
    docstring's "RESIDUAL IMPUTATION" section for why this is not simply
    imported). ``weights`` must be non-empty with a strictly positive total
    — the caller only reaches this after confirming both.
    """
    total = sum(weights.values(), _ZERO)
    if total <= _ZERO:
        raise DescentError(f"cannot normalize shares: total weight is {total} (must be > 0)")
    quantized = {
        dc_id: (weight / total).quantize(_PCT_QUANTUM) for dc_id, weight in weights.items()
    }
    residual = _ONE - sum(quantized.values(), _ZERO)
    if residual != _ZERO:
        target = min(weights, key=lambda dc_id: (-weights[dc_id], str(dc_id)))
        quantized[target] = quantized[target] + residual
    return quantized


def _impute_qty_residual(
    qty_by_dc: dict[_uuid_mod.UUID, Decimal],
    pct_by_dc: dict[_uuid_mod.UUID, Decimal],
    qty_source: Decimal,
) -> dict[_uuid_mod.UUID, Decimal]:
    """Impute the quantization rounding gap (``qty_source -
    SUM(qty_by_dc)``), in full, onto the largest-``pct_by_dc`` entry (ties
    broken by the smallest ``dc_location_id``) — the SAME deterministic rule
    as ``_normalize_pct``, applied to quantities instead of percentages. See
    the module docstring's "RESIDUAL IMPUTATION" section."""
    residual = qty_source - sum(qty_by_dc.values(), _ZERO)
    if residual != _ZERO:
        target = min(pct_by_dc, key=lambda dc_id: (-pct_by_dc[dc_id], str(dc_id)))
        qty_by_dc[target] = qty_by_dc[target] + residual
    return qty_by_dc


def _resolve_item_shares(
    item_ids: list[_uuid_mod.UUID],
    raw_shares: dict[_uuid_mod.UUID, dict[_uuid_mod.UUID, Decimal]],
    eligibility: dict[tuple[_uuid_mod.UUID, _uuid_mod.UUID], bool],
    scenario_id: _uuid_mod.UUID,
) -> dict[_uuid_mod.UUID, dict[_uuid_mod.UUID, Decimal]]:
    """Apply the eligibility gate then renormalize (Sigma=1) per item. An
    item with zero raw shares, or whose every share is ineligible (absent
    from ``item_dc_eligibility`` or explicitly ``eligible=FALSE`` — see the
    module docstring's "ELIGIBILITY GATE" section), is simply ABSENT from the
    return value — the caller treats that as "no usable share" (ADR-043
    fail-loudly: demand stays national)."""
    resolved: dict[_uuid_mod.UUID, dict[_uuid_mod.UUID, Decimal]] = {}
    for item_id in item_ids:
        item_raw = raw_shares.get(item_id)
        if not item_raw:
            continue
        filtered = {
            dc_id: pct
            for dc_id, pct in item_raw.items()
            if eligibility.get((item_id, dc_id), False) is True
        }
        excluded = set(item_raw) - set(filtered)
        if excluded:
            logger.warning(
                "descent.eligibility_excluded scenario_id=%s item_id=%s excluded_dcs=%s",
                scenario_id, item_id, sorted(str(dc_id) for dc_id in excluded),
            )
        if not filtered:
            continue
        resolved[item_id] = _normalize_pct(filtered)
    return resolved


def execute_descent(
    conn: DictRowConnection,
    *,
    scenario_id: _uuid_mod.UUID = BASELINE_SCENARIO_ID,
    now: datetime,
    dry_run: bool = False,
    source: str = "engine",
) -> DescentResult:
    """Run one demand-descent pass for ``scenario_id``.

    ``now`` is caller-injected (never ``datetime.now()`` inside this
    function) so every timestamp this run stamps — ``calc_runs``,
    ``demand_descent_lines``, the derived nodes' ``created_at``/
    ``updated_at``, the deactivated source's ``updated_at`` — is
    deterministic and testable. Production callers pass
    ``datetime.now(timezone.utc)``.

    ``dry_run=True``: every read + computation runs (including the
    eligibility gate and BOTH residual-imputation stages), so
    ``items_without_shares`` and every count in the returned
    ``DescentResult`` are exactly what a real run WOULD produce — but
    nothing is written (no calc_run, no nodes, no edges, no ledger rows, no
    deactivation, no event).

    Never commits, never rolls back — the caller owns the transaction.
    """
    _assert_scenario_exists(conn, scenario_id)

    sources = _fetch_national_demand(conn, scenario_id)
    item_ids = sorted({s.item_id for s in sources}, key=str)
    raw_shares = _fetch_resolved_shares(conn, scenario_id, item_ids)
    eligibility = _fetch_eligibility(conn, item_ids)
    item_shares = _resolve_item_shares(item_ids, raw_shares, eligibility, scenario_id)

    items_without_shares: list[_uuid_mod.UUID] = []
    planned_lines: list[_PlannedLine] = []
    sources_split: list[_SourceNode] = []
    needed_dc_ids: set[_uuid_mod.UUID] = set()

    for src in sources:
        shares = item_shares.get(src.item_id)
        if not shares:
            items_without_shares.append(src.item_id)
            continue

        qty_source_q = src.quantity.quantize(_QTY_QUANTUM)
        qty_by_dc = {
            dc_id: (qty_source_q * pct).quantize(_QTY_QUANTUM) for dc_id, pct in shares.items()
        }
        qty_by_dc = _impute_qty_residual(qty_by_dc, shares, qty_source_q)

        for dc_id in sorted(shares, key=str):
            needed_dc_ids.add(dc_id)
            derived_node_id = _uuid_mod.uuid5(
                _DESCENT_NODE_NAMESPACE, f"{src.node_id}:{dc_id}"
            )
            planned_lines.append(
                _PlannedLine(
                    source=src,
                    dc_location_id=dc_id,
                    pct_applied=shares[dc_id],
                    qty_source=qty_source_q,
                    qty_derived=qty_by_dc[dc_id],
                    derived_node_id=derived_node_id,
                    derived_external_id="",  # filled below, once DC labels are known
                )
            )
        sources_split.append(src)

    dc_external_ids = _fetch_dc_external_ids(conn, needed_dc_ids)
    planned_lines = [
        _PlannedLine(
            source=line.source,
            dc_location_id=line.dc_location_id,
            pct_applied=line.pct_applied,
            qty_source=line.qty_source,
            qty_derived=line.qty_derived,
            derived_node_id=line.derived_node_id,
            derived_external_id=(
                f"{line.source.label}@"
                f"{dc_external_ids.get(line.dc_location_id, str(line.dc_location_id))}"
            ),
        )
        for line in planned_lines
    ]

    items_without_shares_out = tuple(sorted(set(items_without_shares), key=str))

    if dry_run or not planned_lines:
        logger.info(
            "descent.dry_run=%s scenario_id=%s sources_considered=%d lines_planned=%d "
            "items_without_shares=%d",
            dry_run, scenario_id, len(sources), len(planned_lines), len(items_without_shares_out),
        )
        return DescentResult(
            scenario_id=scenario_id,
            dry_run=dry_run,
            descent_run_id=None,
            event_id=None,
            source_nodes_considered=len(sources),
            source_nodes_deactivated=0,
            derived_nodes_created=0,
            lines_written=0,
            items_without_shares=items_without_shares_out,
        )

    # --- WRITE PHASE (dry_run is False, planned_lines is non-empty) ---
    descent_run_id = _uuid_mod.uuid4()
    conn.execute(
        """
        INSERT INTO calc_runs (
            calc_run_id, scenario_id, is_full_recompute, status, started_at, completed_at, created_at
        ) VALUES (%s, %s, FALSE, 'completed', %s, %s, %s)
        """,
        (descent_run_id, scenario_id, now, now, now),
    )

    # NULL = baseline run (mirrors demand_split_pct's own baseline
    # convention — migration 083's "SCENARIO SCOPE" section).
    ledger_scenario_id = None if scenario_id == BASELINE_SCENARIO_ID else scenario_id

    derived_created = 0
    for line in planned_lines:
        src = line.source
        inserted_row = conn.execute(
            """
            INSERT INTO nodes (
                node_id, node_type, scenario_id, item_id, location_id,
                quantity, qty_uom, time_grain, time_ref, time_span_start, time_span_end,
                external_id, is_dirty, active, created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, TRUE, TRUE, %s, %s
            )
            ON CONFLICT (node_id) DO NOTHING
            RETURNING node_id
            """,
            (
                line.derived_node_id, src.node_type, scenario_id, src.item_id, line.dc_location_id,
                line.qty_derived, src.qty_uom, src.time_grain, src.time_ref,
                src.time_span_start, src.time_span_end,
                line.derived_external_id, now, now,
            ),
        ).fetchone()
        if inserted_row is not None:
            derived_created += 1

        # Same graph-wiring path as ingest.py — see graph_wiring.py's module
        # docstring. Projection is NOT recomputed here (see this module's
        # docstring) — only the graph structure (PI series existence + the
        # consumes/replenishes edge) is materialized.
        ensure_projection_series(conn, src.item_id, line.dc_location_id, scenario_id)
        wire_node_to_pi(
            conn,
            line.derived_node_id,
            src.node_type,
            src.item_id,
            line.dc_location_id,
            scenario_id,
            src.time_ref,
            time_span_start=src.time_span_start,
            time_span_end=src.time_span_end,
        )

        conn.execute(
            """
            INSERT INTO demand_descent_lines (
                scenario_id, descent_run_id, source_node_id, derived_node_id,
                item_id, dc_location_id, pct_applied, qty_source, qty_derived, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                ledger_scenario_id, descent_run_id, src.node_id, line.derived_node_id,
                src.item_id, line.dc_location_id, line.pct_applied,
                line.qty_source, line.qty_derived, now,
            ),
        )

    # Anti-double-count: deactivate every split source in ONE statement
    # (audit retained — active=FALSE, never deleted, per ADR-043 §1 point 3).
    deactivated_ids = [s.node_id for s in sources_split]
    conn.execute(
        "UPDATE nodes SET active = FALSE, is_dirty = TRUE, updated_at = %s WHERE node_id = ANY(%s)",
        (now, deactivated_ids),
    )

    # Invariant #455 — see module docstring. Applied defensively, once, in
    # bulk, at the end of the write phase (mirrors purge.py's _analyze_tables).
    conn.execute("ANALYZE nodes")

    old_text = ",".join(str(i) for i in items_without_shares_out) or None
    event_id = emit_stream_event(
        conn,
        "demand_descended",
        scenario_id,
        field_changed="demand_descended",
        new_text=str(descent_run_id),
        new_quantity=len(planned_lines),
        old_text=old_text,
        source=source,
    )

    logger.info(
        "descent.applied scenario_id=%s descent_run_id=%s sources_considered=%d "
        "sources_deactivated=%d derived_created=%d lines_written=%d items_without_shares=%d",
        scenario_id, descent_run_id, len(sources), len(sources_split),
        derived_created, len(planned_lines), len(items_without_shares_out),
    )

    return DescentResult(
        scenario_id=scenario_id,
        dry_run=False,
        descent_run_id=descent_run_id,
        event_id=event_id,
        source_nodes_considered=len(sources),
        source_nodes_deactivated=len(sources_split),
        derived_nodes_created=derived_created,
        lines_written=len(planned_lines),
        items_without_shares=items_without_shares_out,
    )


def _assert_scenario_exists(conn: DictRowConnection, scenario_id: _uuid_mod.UUID) -> None:
    row = conn.execute(
        "SELECT 1 FROM scenarios WHERE scenario_id = %s", (scenario_id,)
    ).fetchone()
    if row is None:
        raise DescentError(f"scenario {scenario_id} does not exist")
