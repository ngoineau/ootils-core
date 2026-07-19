"""
detector.py — ShortageDetector for Sprint M4.

Detects shortages from ProjectedInventory nodes (closing_stock < 0),
scores their severity, persists them, and manages lifecycle (resolve stale).

This module is the exclusive owner of the `shortages` table and may use
direct SQL on it.  All other graph data access goes through GraphStore.
"""
from __future__ import annotations

import logging
from decimal import Decimal
from typing import Optional
from uuid import UUID

from ootils_core.engine.kernel._clock import Clock, SystemClock
from ootils_core.engine.kernel._ids import deterministic_uuid
from ootils_core.engine.kernel.shortage.policy import SafetyScope, VALID_SAFETY_SCOPES
from ootils_core.models import Node, ShortageRecord

logger = logging.getLogger(__name__)

# Fallback unit cost for UNPRICED items only (#342). The real valuation flows
# in via detect_with_params(unit_cost=...): the propagator batch-loads it with
# the same precedence as mrp_core.cost_of (negotiated supplier unit_cost, then
# items.standard_cost) so kernel severity and watcher valuation agree. The SQL
# engine mirrors this in propagator_sql.SHORTAGES_SQL — keep all three in sync.
_UNIT_COST_PROXY = Decimal("1")

# Shortage sign-test tolerance. A "stockout" is closing_stock < 0, but the
# Python kernel (Decimal, 28 sig digits) and the SQL engine (numeric(50,28))
# round multi-day demand proration differently in the ~24-26th digit. On nodes
# whose closing lands at ~0 that sub-1e-12 difference straddles zero, making the
# two engines disagree on the boolean stockout flag (a -1e-13 closing is not a
# real shortage). Treating |closing| < EPS as "effectively zero stock" aligns
# both engines deterministically. EPS is parts-per-billion — ~9 orders of
# magnitude below any business-meaningful inventory quantity, far above the
# 1e-12 rounding noise. The SQL engine uses the same literal (-1e-9); keep them
# in sync. See docs/PERF-BASELINE.md "has_shortage au bord de zéro".
SHORTAGE_EPSILON = Decimal("1e-9")


class ShortageDetector:
    """
    Detects, scores, persists, and resolves inventory shortages.

    Owns the `shortages` table — uses direct SQL.

    Optional ``clock`` (ADR-003): pass a ``FrozenClock`` from tests so
    ``created_at`` / ``updated_at`` values are reproducible.
    """

    def __init__(self, clock: Clock | None = None) -> None:
        self._clock = clock or SystemClock()

    # ------------------------------------------------------------------
    # Detection
    # ------------------------------------------------------------------

    def detect(
        self,
        pi_node: Node,
        calc_run_id: UUID,
        scenario_id: UUID,
        db,
        is_stocking: bool = True,
    ) -> Optional[ShortageRecord]:
        """
        Inspect a PI node and return a ShortageRecord if closing_stock < 0.

        Returns None if no shortage exists.
        Delegates to detect_with_params with no safety stock.
        """
        return self.detect_with_params(
            pi_node=pi_node,
            calc_run_id=calc_run_id,
            scenario_id=scenario_id,
            db=db,
            is_stocking=is_stocking,
        )

    def detect_with_params(
        self,
        pi_node: Node,
        calc_run_id: UUID,
        scenario_id: UUID,
        db,
        safety_stock_qty: Optional[Decimal] = None,
        unit_cost: Optional[Decimal] = None,
        is_stocking: bool = True,
        safety_scope: SafetyScope = "per_site",
    ) -> Optional[ShortageRecord]:
        """
        Enhanced detection: detects both stockouts (closing_stock < 0) and
        below-safety-stock warnings (closing_stock < safety_stock_qty).

        Returns ShortageRecord with severity_class:
        - 'stockout': closing_stock < 0
        - 'below_safety_stock': 0 <= closing_stock < safety_stock_qty
        - None: no shortage

        severity_score = shortage_qty × days_in_bucket × unit_cost (or proxy)

        ``is_stocking`` (migration 081, PR-B — virtual demand-channel
        exclusion): mirrors the SQL engine's `pi_with_ss` CTE in
        `propagator_sql.SHORTAGES_SQL`. Locations flagged `is_stocking=FALSE`
        (virtual routing/allocation channels carrying demand but no supply of
        any kind) never materialize a `shortages` row — this is DETECTION
        gating only. The PROJECTION (`pi_node.closing_stock` etc.) is computed
        upstream regardless of this flag (explainability, ADR-004); a caller
        that skips detection here still has the negative closing stock
        visible on the PI node itself. Default True preserves existing
        behaviour for every location that hasn't opted out.

        ``safety_scope`` (ADR-021 amendment, DESC-1 PR-C, pilot arbitration
        2026-07-18 — `engine.kernel.shortage.policy`): strict mirror of the
        SQL engine's `pi_with_ss` CTE in `propagator_sql.SHORTAGES_SQL`. In
        'national' scope, the caller-supplied ``safety_stock_qty`` is
        ignored entirely (treated as absent, NOT coerced to 0 — a literal 0
        would still trip the below_safety_stock branch for the
        ``[-EPS, 0)`` rounding-noise sliver that ``SHORTAGE_EPSILON`` exists
        to absorb, leaking a near-zero shortage row even in national scope):
        only a physical stockout (``closing_stock < -EPS``) can fire. Default
        'per_site' preserves the pre-DESC-1 behaviour byte-for-byte for any
        caller unaware of this axis (existing tests, any future standalone
        caller) — the ONE production call site
        (``PropagationEngine._recompute_pi_node``, orchestration/propagator.py)
        always resolves the real policy once per calc_run
        (``policy.safety_scope()``) and passes it explicitly, exactly how
        ``is_stocking`` is threaded through the same call site.
        """
        if safety_scope not in VALID_SAFETY_SCOPES:
            raise ValueError(
                f"Invalid safety_scope={safety_scope!r}; expected one of "
                f"{VALID_SAFETY_SCOPES}"
            )

        if not is_stocking:
            logger.debug(
                "Shortage detection skipped: location not stocking (node=%s)",
                pi_node.node_id,
            )
            return None

        closing = pi_node.closing_stock
        if closing is None:
            return None

        effective_unit_cost = unit_cost if unit_cost is not None else _UNIT_COST_PROXY

        # National scope ignores the per-site threshold entirely (see the
        # docstring above) — never coerced to 0, only ever nulled out.
        effective_safety_stock_qty = (
            None if safety_scope == "national" else safety_stock_qty
        )

        # Determine severity_class and shortage_qty. The -EPS / +EPS boundary
        # keeps the sign test deterministic across the Python and SQL engines
        # (see SHORTAGE_EPSILON above): a closing within ±EPS of zero is treated
        # as effectively zero stock, never a stockout.
        if closing < -SHORTAGE_EPSILON:
            severity_class = "stockout"
            shortage_qty = abs(closing)
        elif (
            effective_safety_stock_qty is not None
            and -SHORTAGE_EPSILON <= closing < effective_safety_stock_qty
        ):
            severity_class = "below_safety_stock"
            shortage_qty = effective_safety_stock_qty - closing
        else:
            return None

        # Bucket duration
        if pi_node.time_span_start is not None and pi_node.time_span_end is not None:
            days_in_bucket = (pi_node.time_span_end - pi_node.time_span_start).days
            if days_in_bucket <= 0:
                days_in_bucket = 1
        else:
            days_in_bucket = 1

        severity_score = shortage_qty * Decimal(str(days_in_bucket)) * effective_unit_cost

        # May be None when the PI node has no time coordinate at all (unit
        # tests exercise this in-memory). ShortageRecord.shortage_date is
        # Optional[date]; the shortages.shortage_date NOT NULL column rejects a
        # None only at persist time — the pre-existing behaviour, unchanged.
        shortage_date = pi_node.time_span_start or pi_node.time_ref

        # Drive timestamps from the injected clock so the record is fully
        # deterministic (ADR-003). The dataclass defaults to wall-clock
        # datetime.now() — overridden here.
        now = self._clock.now()
        record = ShortageRecord(
            shortage_id=deterministic_uuid(
                "shortage", scenario_id, calc_run_id, pi_node.node_id,
            ),
            scenario_id=scenario_id,
            pi_node_id=pi_node.node_id,
            item_id=pi_node.item_id,
            location_id=pi_node.location_id,
            shortage_date=shortage_date,
            shortage_qty=shortage_qty,
            severity_score=severity_score,
            explanation_id=None,  # linked post-build by ExplanationBuilder if needed
            calc_run_id=calc_run_id,
            status="active",
            severity_class=severity_class,
            created_at=now,
            updated_at=now,
        )

        logger.debug(
            "Shortage detected on node %s — qty=%s severity=%s class=%s",
            pi_node.node_id,
            shortage_qty,
            severity_score,
            severity_class,
        )
        return record

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def persist(self, shortage: ShortageRecord, db) -> None:
        """
        Upsert a ShortageRecord into the `shortages` table.
        ON CONFLICT (pi_node_id, calc_run_id) → update all mutable fields.
        """
        now = self._clock.now()
        db.execute(
            """
            INSERT INTO shortages (
                shortage_id,
                scenario_id,
                pi_node_id,
                item_id,
                location_id,
                shortage_date,
                shortage_qty,
                severity_score,
                explanation_id,
                calc_run_id,
                status,
                severity_class,
                created_at,
                updated_at
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s
            )
            ON CONFLICT (pi_node_id, calc_run_id) DO UPDATE SET
                shortage_qty    = EXCLUDED.shortage_qty,
                severity_score  = EXCLUDED.severity_score,
                shortage_date   = EXCLUDED.shortage_date,
                explanation_id  = EXCLUDED.explanation_id,
                status          = EXCLUDED.status,
                severity_class  = EXCLUDED.severity_class,
                updated_at      = EXCLUDED.updated_at
            """,
            (
                shortage.shortage_id,
                shortage.scenario_id,
                shortage.pi_node_id,
                shortage.item_id,
                shortage.location_id,
                shortage.shortage_date,
                shortage.shortage_qty,
                shortage.severity_score,
                shortage.explanation_id,
                shortage.calc_run_id,
                shortage.status,
                shortage.severity_class,
                shortage.created_at,
                now,
            ),
        )
        shortage.updated_at = now
        logger.debug("Shortage persisted: %s", shortage.shortage_id)

    # ------------------------------------------------------------------
    # Lifecycle management
    # ------------------------------------------------------------------

    def resolve_stale(
        self,
        scenario_id: UUID,
        calc_run_id: UUID,
        db,
    ) -> int:
        """
        Mark as 'resolved' the active shortages that BELONG to a series this
        run recalculated but were NOT re-detected in it — i.e. the shortage
        cleared. Resolution is scoped to the series ``calc_run_id`` actually
        recomputed, never the whole scenario blindly.

        Scoping (chantier C3 « moteur d'exception », 2026-07-19): a shortage is
        retired ONLY when its ProjectedInventory node carries
        ``last_calc_run_id = calc_run_id`` — the uniform stamp BOTH engines write
        on every PI they recompute (Python: ``GraphStore.update_pi_result`` /
        ``update_pi_results``, store.py:423/475; SQL: ``PROPAGATE_SQL``,
        propagator_sql.py:233). This is the STRICT generalisation of the
        historical full-run behaviour, and the prerequisite for the incremental
        daily run:

          * FULL recompute — every active PI carries the current run, so the
            in-scope set is the whole scenario: byte-for-byte the pre-C3
            behaviour (the ``calc_run_id != %s`` clause alone decided it, and
            the new EXISTS is universally true).
          * PARTIAL / incremental run — only the recomputed series are in scope;
            a shortage on a series this run never touched keeps an earlier
            ``last_calc_run_id`` and is LEFT UNTOUCHED instead of being wrongly
            resolved. This also neutralises, BY CONSTRUCTION, the corruption
            path where propagation is skipped entirely (an event with no trigger
            node — ``POST /v1/calc/run`` with ``full_recompute=false``): zero PI
            gets stamped, so zero shortage is resolved, rather than every active
            shortage in the scenario being silently resolved and none
            re-detected.

        ``ShortageDetector`` stays the exclusive writer of the ``shortages``
        table and its resolution lifecycle (ADR-021).

        Returns the count of rows updated.
        """
        now = self._clock.now()
        result = db.execute(
            """
            UPDATE shortages AS s
            SET status     = 'resolved',
                updated_at = %s
            WHERE s.scenario_id  = %s
              AND s.status        = 'active'
              AND s.calc_run_id  != %s
              AND EXISTS (
                  SELECT 1
                  FROM nodes n
                  WHERE n.node_id          = s.pi_node_id
                    AND n.scenario_id      = s.scenario_id
                    AND n.last_calc_run_id = %s
              )
            """,
            (now, scenario_id, calc_run_id, calc_run_id),
        )
        count = result.rowcount if hasattr(result, "rowcount") else 0
        logger.info(
            "resolve_stale: %d shortages resolved for scenario %s (calc_run %s)",
            count,
            scenario_id,
            calc_run_id,
        )
        return count

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def get_active_shortages(
        self,
        scenario_id: UUID,
        db,
    ) -> list[ShortageRecord]:
        """
        Return all active shortages for a scenario, sorted by severity_score DESC.
        """
        rows = db.execute(
            """
            SELECT
                shortage_id,
                scenario_id,
                pi_node_id,
                item_id,
                location_id,
                shortage_date,
                shortage_qty,
                severity_score,
                explanation_id,
                calc_run_id,
                status,
                severity_class,
                created_at,
                updated_at
            FROM shortages
            WHERE scenario_id = %s
              AND status = 'active'
            ORDER BY severity_score DESC
            """,
            (scenario_id,),
        ).fetchall()

        return [_row_to_shortage(r) for r in rows]


# ------------------------------------------------------------------
# Row → domain model helper
# ------------------------------------------------------------------


def _row_to_shortage(row) -> ShortageRecord:
    """Convert a DB row (dict or dict-like) to a ShortageRecord."""
    return ShortageRecord(
        shortage_id=UUID(str(row["shortage_id"])),
        scenario_id=UUID(str(row["scenario_id"])),
        pi_node_id=UUID(str(row["pi_node_id"])),
        item_id=UUID(str(row["item_id"])) if row.get("item_id") else None,
        location_id=UUID(str(row["location_id"])) if row.get("location_id") else None,
        shortage_date=row["shortage_date"],
        shortage_qty=Decimal(str(row["shortage_qty"])),
        severity_score=Decimal(str(row["severity_score"])),
        explanation_id=UUID(str(row["explanation_id"])) if row.get("explanation_id") else None,
        calc_run_id=UUID(str(row["calc_run_id"])),
        status=row["status"],
        severity_class=row.get("severity_class"),
        created_at=row.get("created_at"),
        updated_at=row.get("updated_at"),
    )
