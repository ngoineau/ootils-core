"""
propagator_sql.py — SQL-backed propagation engine (Tier 3, REVIEW-2026-05).

`SqlPropagationEngine` inherits from `PropagationEngine` and replaces only
the `_propagate` step with a window-function UPDATE inside Postgres. The
boundary contract is unchanged: same `process_event`, same advisory lock,
same `calc_run` lifecycle, same dirty-subgraph expansion via
`GraphTraversal`. Only the per-bucket compute moves from Python to SQL.

Performance ceiling vs. the Python kernel (measured on the parity harness):
- Full recompute @ 600 PI / 177 shortages: ~4.5x faster (1.0s -> 0.22s)
- Full recompute @ 12K PI / 2.5K shortages: ~1.7x faster (current; see
  docs/SCALABILITY.md "Tier 3 spike" for the perf landscape and remaining
  optimisation opportunities)

The two engines produce byte-identical results within Decimal precision
tolerance (1e-12), validated by `scripts/parity_sql_vs_python.py`.

Explainability (M3) is preserved via **lazy generation** at read time:
- `GET /v1/explain/{node_id}` regenerates the causal chain on the fly
  if it isn't already in DB for the requested node + shortage.
- This avoids the bulk-generation cost (24K shortages × ~10ms each on
  profile S) for explanations that are never consulted.
- The contract "every result is explainable" is satisfied without
  paying the eager generation tax on every propagation.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from uuid import UUID

from ootils_core.db.types import DictRowConnection
from ootils_core.engine.kernel.shortage.policy import is_national_scope
from ootils_core.engine.orchestration.propagator import PropagationEngine
from ootils_core.engine.scenario.param_overlay import resolved_field_lateral_sql

if TYPE_CHECKING:
    from ootils_core.models import CalcRun

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SQL — kept inline as constants so a DBA can read the propagation algorithm
# without having to spelunk Python. Schema-coupled; touch with care.
# ---------------------------------------------------------------------------

PROPAGATE_SQL = """
-- Tier 3 propagation: window-function projection over the dirty subgraph.
-- Each affected projection_series is processed from its lowest dirty bucket
-- (the "seed"); subsequent dirty buckets chain via the window function.
-- ASSUMES dirty buckets within a series are contiguous (guaranteed by
-- expand_dirty_subgraph's downstream cascade through feeds_forward edges).
WITH dirty_pi AS (
    SELECT pi.node_id, pi.projection_series_id, pi.bucket_sequence,
           pi.time_span_start, pi.time_span_end, pi.scenario_id
    FROM nodes pi
    JOIN dirty_nodes dn
      ON dn.node_id = pi.node_id
     AND dn.scenario_id = pi.scenario_id
    WHERE dn.calc_run_id = %(calc_run_id)s
      AND pi.node_type = 'ProjectedInventory'
      AND pi.scenario_id = %(scenario_id)s
      AND pi.active = TRUE
),
series_first_dirty AS (
    SELECT projection_series_id, MIN(bucket_sequence) AS seed_seq
    FROM dirty_pi
    GROUP BY projection_series_id
),
seed_openings AS (
    SELECT
        sfd.projection_series_id,
        sfd.seed_seq,
        CASE WHEN sfd.seed_seq = 0 THEN
            COALESCE((
                SELECT SUM(oh.quantity)
                FROM nodes pi_seed
                JOIN edges r ON r.to_node_id = pi_seed.node_id
                JOIN nodes oh ON oh.node_id = r.from_node_id
                WHERE pi_seed.projection_series_id = sfd.projection_series_id
                  AND pi_seed.bucket_sequence = 0
                  AND pi_seed.scenario_id = %(scenario_id)s
                  AND pi_seed.active = TRUE
                  AND r.edge_type = 'replenishes'
                  AND r.scenario_id = %(scenario_id)s
                  AND r.active = TRUE
                  AND oh.node_type = 'OnHandSupply'
                  AND oh.active = TRUE
            ), 0)::numeric
        ELSE
            COALESCE((
                SELECT prev.closing_stock
                FROM nodes prev
                WHERE prev.projection_series_id = sfd.projection_series_id
                  AND prev.bucket_sequence = sfd.seed_seq - 1
                  AND prev.scenario_id = %(scenario_id)s
                  AND prev.active = TRUE
            ), 0)::numeric
        END AS seed_opening
    FROM series_first_dirty sfd
),
-- inflows + outflows were originally two CORRELATED scalar subqueries
-- evaluated once PER dirty PI (~2 per row → ~450K executions at 227K PI),
-- the dominant cost behind the documented S→M→L throughput collapse
-- (docs/PERF-BASELINE.md). They are de-correlated below into two SEPARATE
-- aggregate CTEs scanned once over the whole dirty subgraph, then LEFT JOINed
-- back. Kept as two distinct CTEs on purpose: a single JOIN spanning both
-- edge types would cross-multiply inflow×outflow rows and double-count.
-- The per-row expressions (numeric(50,28) Decimal-precision cast, the
-- consumes prorating CASE) are preserved verbatim so parity stays bit-exact.
inflows_agg AS (
    SELECT dp.node_id, SUM(s.quantity) AS inflows
    FROM dirty_pi dp
    JOIN edges r
      ON r.to_node_id = dp.node_id
     AND r.edge_type = 'replenishes'
     AND r.scenario_id = dp.scenario_id
     AND r.active = TRUE
    JOIN nodes s
      ON s.node_id = r.from_node_id
     AND s.node_type IN ('PurchaseOrderSupply','WorkOrderSupply','TransferSupply','PlannedSupply')
     AND s.active = TRUE
     AND s.time_ref >= dp.time_span_start
     AND s.time_ref <  dp.time_span_end
    GROUP BY dp.node_id
),
-- outflow_contribs computes the SAME per-row prorated quantity as before
-- (verbatim CASE expression — parity stays bit-exact), kept at row grain so
-- outflows_agg can split the aggregate by demand node_type: forecast and
-- firm customer-order demand for the SAME bucket are NOT additive (a CO
-- fulfils the forecast it was booked against — netted via GREATEST below),
-- while dependent/transfer demand IS additive (derived/moved supply need,
-- never a forecast a CO could be consuming). Mirrors propagator.py's Python
-- netting (`_recompute_pi_node`); `engine/mrp/core.py`'s `max(o, f)` already
-- netted Truth B (ADR-021 convergence, 2026-07-17) — core.py is untouched.
outflow_contribs AS (
    SELECT
        dp.node_id,
        d.node_type,
        CASE
            WHEN d.time_span_start IS NOT NULL
                 AND d.time_span_end IS NOT NULL
                 AND d.time_span_end > d.time_span_start THEN
                -- numeric(50,28) cast matches Python Decimal default precision
                d.quantity::numeric(50, 28)
                / (d.time_span_end - d.time_span_start)::numeric
                * GREATEST(
                    0,
                    LEAST(dp.time_span_end, d.time_span_end)
                    - GREATEST(dp.time_span_start, d.time_span_start)
                  )::numeric
            WHEN COALESCE(d.time_ref, d.time_span_start) IS NOT NULL
                 AND COALESCE(d.time_ref, d.time_span_start) >= dp.time_span_start
                 AND COALESCE(d.time_ref, d.time_span_start) <  dp.time_span_end THEN
                d.quantity
            ELSE 0
        END AS prorated_qty
    FROM dirty_pi dp
    JOIN edges c
      ON c.to_node_id = dp.node_id
     AND c.edge_type = 'consumes'
     AND c.scenario_id = dp.scenario_id
     AND c.active = TRUE
    JOIN nodes d
      ON d.node_id = c.from_node_id
     AND d.node_type IN ('ForecastDemand','CustomerOrderDemand','DependentDemand','TransferDemand')
     AND d.active = TRUE
),
outflows_agg AS (
    SELECT
        node_id,
        SUM(prorated_qty) FILTER (WHERE node_type = 'ForecastDemand')      AS fc_out,
        SUM(prorated_qty) FILTER (WHERE node_type = 'CustomerOrderDemand') AS co_out,
        SUM(prorated_qty) FILTER (WHERE node_type IN ('DependentDemand','TransferDemand')) AS dep_out
    FROM outflow_contribs
    GROUP BY node_id
),
per_bucket AS (
    SELECT
        dp.node_id,
        dp.projection_series_id,
        dp.bucket_sequence,
        dp.time_span_start,
        dp.time_span_end,
        CASE WHEN dp.bucket_sequence = so.seed_seq THEN so.seed_opening
             ELSE 0::numeric END AS oh_seed,
        COALESCE(ia.inflows, 0)::numeric AS inflows,
        -- GREATEST(fc, co) + dep: dependent/transfer demand always adds,
        -- forecast vs CO are netted (never summed) for the same bucket.
        (GREATEST(COALESCE(oa.fc_out, 0), COALESCE(oa.co_out, 0))
            + COALESCE(oa.dep_out, 0))::numeric AS outflows
    FROM dirty_pi dp
    JOIN seed_openings so ON so.projection_series_id = dp.projection_series_id
    LEFT JOIN inflows_agg  ia ON ia.node_id = dp.node_id
    LEFT JOIN outflows_agg oa ON oa.node_id = dp.node_id
),
projected AS (
    SELECT
        node_id,
        bucket_sequence,
        inflows,
        outflows,
        SUM(oh_seed) OVER (
            PARTITION BY projection_series_id ORDER BY bucket_sequence
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        + COALESCE(SUM(inflows - outflows) OVER (
            PARTITION BY projection_series_id ORDER BY bucket_sequence
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0) AS opening_stock
    FROM per_bucket
)
UPDATE nodes
SET opening_stock = p.opening_stock,
    inflows       = p.inflows,
    outflows      = p.outflows,
    closing_stock = p.opening_stock + p.inflows - p.outflows,
    -- Mirrors ProjectionKernel: a "negative closing stock" shortage.
    -- Safety-stock-based shortages are detected in SHORTAGES_SQL below.
    -- The -1e-9 tolerance matches SHORTAGE_EPSILON in the Python kernel
    -- (engine/kernel/shortage/detector.py): a closing within ±1e-9 of zero is
    -- effectively-zero stock, not a stockout — keeps the sign test deterministic
    -- across both engines at the ~0 boundary. See docs/PERF-BASELINE.md.
    has_shortage  = (p.opening_stock + p.inflows - p.outflows) < -1e-9,
    shortage_qty  = CASE
                        WHEN (p.opening_stock + p.inflows - p.outflows) < -1e-9
                            THEN -(p.opening_stock + p.inflows - p.outflows)
                        ELSE 0
                    END,
    is_dirty      = FALSE,
    last_calc_run_id = %(calc_run_id)s,
    updated_at    = now()
FROM projected p
WHERE nodes.node_id = p.node_id;
"""


SHORTAGES_SQL = """
-- Mirror ShortageDetector.detect_with_params + persist in pure SQL.
--   closing < -EPS                         -> 'stockout',           qty = -closing
--   -EPS <= closing < safety_stock_qty     -> 'below_safety_stock', qty = safety_stock - closing
-- EPS = 1e-9, matching SHORTAGE_EPSILON in the Python kernel (deterministic
-- sign test across engines at the ~0 boundary; see docs/PERF-BASELINE.md).
-- severity_score = shortage_qty * days_in_bucket * unit_cost (#342).
-- unit_cost mirrors mrp_core.cost_of — the single valuation precedence used
-- by the watcher fleet: negotiated supplier unit_cost (preferred supplier,
-- cheapest priced row) first, then items.standard_cost (migration 042 BOM
-- roll-up), then 1 (the Python detector's _UNIT_COST_PROXY fallback for
-- unpriced items).
-- shortage_date  = COALESCE(time_span_start, time_ref)
-- Restricted to the dirty subgraph for this calc_run; shortages on PIs this
-- run did NOT recompute are retired by ShortageDetector.resolve_stale at
-- end-of-run (engine/kernel/shortage/detector.py) — the single live
-- stale-resolution path, scoped by nodes.last_calc_run_id.
-- safety_stock_qty (chantier #347 PR3, ADR-025): resolved through
-- resolved_field_lateral_sql() instead of a raw item_planning_params
-- LATERAL, so a scenario-scoped safety_stock_qty override (set via
-- set_param_override()) is visible to shortage detection inside a fork.
-- %(scenario_id)s here is the SAME parameter already bound for the PI
-- filter below (`pi.scenario_id = %(scenario_id)s`) — psycopg3 binds a
-- repeated named placeholder from a single dict key, and this query needs
-- exactly one scenario_id value for BOTH purposes: unlike the Python
-- engine's preload query (propagator.py), which resolves overrides in a
-- query separate from the PI scenario filter and can afford to translate
-- baseline -> None, this single combined query cannot use two different
-- values for the same placeholder. Passing the real (non-translated)
-- baseline UUID here is still baseline-pure: set_param_override() refuses
-- overrides on the baseline scenario (is_baseline=TRUE), so
-- scenario_planning_overrides can structurally never carry a row for it —
-- the LATERAL degrades to "no override row" exactly like scenario_id=NULL
-- would, just via "no matching row" instead of "NULL never equals NOT NULL".
-- Ownership of `shortages` stays exclusively ShortageDetector's (ADR-021);
-- this only corrects the VALUE the query reads, never who writes the row.
--
-- is_stocking (migration 081, PR-B — virtual demand-channel exclusion): the
-- `locations l` LEFT JOIN below + the COALESCE guard in the WHERE clause
-- gate DETECTION only — a location explicitly flagged `is_stocking=FALSE`
-- (a virtual routing/allocation channel carrying demand but no supply of
-- any kind) never produces a `shortages` row. The PROJECTION (PROPAGATE_SQL
-- above) is computed for every location regardless of this flag
-- (explainability, ADR-004) — only this INSERT is gated. LEFT JOIN (not
-- INNER) + COALESCE(l.is_stocking, TRUE) so a PI whose location_id is NULL
-- (nodes.location_id has no NOT NULL constraint) or missing from
-- `locations` degrades to the migration's own DEFAULT TRUE instead of being
-- silently dropped from detection by an unrelated join miss. Mirrored in
-- the Python engine by ShortageDetector.detect_with_params(is_stocking=...)
-- (engine/kernel/shortage/detector.py) — keep both in sync.
--
-- safety_scope (ADR-021 amendment, DESC-1 PR-C, pilot arbitration
-- 2026-07-18, engine/kernel/shortage/policy.py): in 'national' scope, the
-- per-site safety_stock_qty resolved below is a planning/execution
-- artefact, not a detection threshold (the national safety cushion lives
-- in Truth B, engine/mrp/loader.py, unchanged) — %(safety_scope_national)s
-- NULLs it out here so the below_safety_stock branch in shortage_rows
-- (guarded by `safety_stock_qty IS NOT NULL`) can never fire, leaving only
-- the closing_stock < -EPS physical-stockout branch active. NULL, not a
-- literal 0: a literal 0 would still satisfy
-- `closing_stock >= -EPS AND closing_stock < safety_stock_qty` for the
-- [-EPS, 0) sliver — precisely the rounding-noise band EPS exists to
-- absorb (SHORTAGE_EPSILON, detector.py) — which would leak a near-zero
-- below_safety_stock row even in national scope. %(safety_scope_national)s
-- is resolved once per calc_run by policy.is_national_scope() (see
-- shortage_params() below); mirrored in the Python engine by
-- ShortageDetector.detect_with_params(safety_scope=...) — keep both in
-- sync. The Rust in-process engine (propagator_rust.py) inherits this
-- unchanged: both its small-set fallback and its post-Rust-compute pass
-- run this SAME SHORTAGES_SQL string on Python's session.
WITH pi_with_ss AS (
    SELECT
        pi.scenario_id,
        pi.node_id        AS pi_node_id,
        pi.item_id,
        pi.location_id,
        pi.closing_stock,
        COALESCE(pi.time_span_start, pi.time_ref) AS shortage_date,
        GREATEST((pi.time_span_end - pi.time_span_start), 1) AS days_in_bucket,
        CASE WHEN %(safety_scope_national)s THEN NULL::numeric
             ELSE ipp_ss.ipp_ss END AS safety_stock_qty,
        COALESCE(sup.unit_cost, i.standard_cost, 1)::numeric AS unit_cost
    FROM nodes pi
    JOIN dirty_nodes dn
      ON dn.node_id = pi.node_id
     AND dn.scenario_id = pi.scenario_id
    LEFT JOIN locations l
      ON l.location_id = pi.location_id
    LEFT JOIN items i
      ON i.item_id = pi.item_id
    LEFT JOIN LATERAL (
        SELECT unit_cost
        FROM supplier_items
        WHERE item_id = pi.item_id
          AND unit_cost IS NOT NULL AND unit_cost > 0
        ORDER BY is_preferred DESC, unit_cost ASC
        LIMIT 1
    ) sup ON TRUE
    -- This inner LATERAL exposes item_id/location_id so the overlay
    -- LATERAL below can correlate on them. It pins location_id = pi.location_id
    -- and item_planning_params.location_id is NOT NULL, so ipp.location_id is
    -- always the PI's exact location — that is what makes the overlay's
    -- exact-location > item-global precedence resolve correctly here. If a
    -- future migration makes item_planning_params.location_id nullable, or
    -- this filter loosens, revisit the correlation below.
    LEFT JOIN LATERAL (
        SELECT item_id, location_id, safety_stock_qty
        FROM item_planning_params
        WHERE item_id = pi.item_id
          AND location_id = pi.location_id
          AND (effective_to IS NULL OR effective_to = '9999-12-31'::DATE)
        ORDER BY effective_from DESC
        LIMIT 1
    ) ipp ON TRUE
    {ipp_ss_lateral}
    WHERE dn.calc_run_id = %(calc_run_id)s
      AND pi.node_type = 'ProjectedInventory'
      AND pi.scenario_id = %(scenario_id)s
      AND pi.active = TRUE
      AND pi.closing_stock IS NOT NULL
      AND COALESCE(l.is_stocking, TRUE) = TRUE
),
shortage_rows AS (
    SELECT
        gen_random_uuid()::uuid AS shortage_id,
        scenario_id,
        pi_node_id,
        item_id,
        location_id,
        shortage_date,
        CASE
            WHEN closing_stock < -1e-9 THEN -closing_stock
            ELSE safety_stock_qty - closing_stock
        END AS shortage_qty,
        CASE
            WHEN closing_stock < -1e-9 THEN 'stockout'
            ELSE 'below_safety_stock'
        END AS severity_class,
        days_in_bucket,
        unit_cost
    FROM pi_with_ss
    WHERE closing_stock < -1e-9
       OR (
            safety_stock_qty IS NOT NULL
            AND closing_stock >= -1e-9
            AND closing_stock < safety_stock_qty
       )
)
INSERT INTO shortages (
    shortage_id, scenario_id, pi_node_id, item_id, location_id,
    shortage_date, shortage_qty, severity_score,
    explanation_id, calc_run_id, status, severity_class,
    created_at, updated_at
)
SELECT
    shortage_id, scenario_id, pi_node_id, item_id, location_id,
    shortage_date, shortage_qty, shortage_qty * days_in_bucket * unit_cost,
    NULL::uuid, %(calc_run_id)s, 'active', severity_class,
    now(), now()
FROM shortage_rows
ON CONFLICT (pi_node_id, calc_run_id) DO UPDATE SET
    shortage_qty   = EXCLUDED.shortage_qty,
    severity_score = EXCLUDED.severity_score,
    shortage_date  = EXCLUDED.shortage_date,
    status         = EXCLUDED.status,
    severity_class = EXCLUDED.severity_class,
    updated_at     = EXCLUDED.updated_at;
""".format(ipp_ss_lateral=resolved_field_lateral_sql("safety_stock_qty", "ipp", "ipp_ss"))


# NOTE: the end-of-run stale-shortage resolution is NOT done in SQL here — the
# single live path is ShortageDetector.resolve_stale, called from
# PropagationEngine._finish_run for every flavour (sql/python/rust). A former
# RESOLVE_STALE_SQL constant lived here but was dead (never executed); removed
# in chantier C3 (2026-07-19) when resolve_stale gained last_calc_run_id
# scoping, which a scenario-wide SQL UPDATE could not express.


CLEAR_DIRTY_SQL = """
DELETE FROM dirty_nodes
WHERE calc_run_id = %(calc_run_id)s AND scenario_id = %(scenario_id)s
"""


def shortage_params(scenario_id: UUID, calc_run_id: UUID) -> dict[str, object]:
    """
    Build the params dict shared by PROPAGATE_SQL / SHORTAGES_SQL / CLEAR_DIRTY_SQL.

    Resolves `is_national_scope()` ONCE here — the single point where a
    misconfigured `OOTILS_SAFETY_SCOPE` fails loudly (`ValueError`), right
    at the start of a calc run rather than deep inside a query. PROPAGATE_SQL
    and CLEAR_DIRTY_SQL don't reference `safety_scope_national` — psycopg
    silently ignores unused keys in a named-parameter mapping — so this one
    dict is safe to reuse, unchanged, across all three statements. Shared
    by `SqlPropagationEngine._propagate` and both shortage-detection call
    sites in `propagator_rust.py` (`_propagate_via_sql` / `_propagate_via_rust`).
    """
    return {
        "scenario_id": scenario_id,
        "calc_run_id": calc_run_id,
        "safety_scope_national": is_national_scope(),
    }


class SqlPropagationEngine(PropagationEngine):
    """
    Propagation engine that delegates the hot path to PostgreSQL.

    Subclasses `PropagationEngine` and overrides only `_propagate`. The rest
    of the lifecycle (advisory lock, calc_run start/finish, dirty subgraph
    expansion, scenario resolution) is reused unchanged.
    """

    def _propagate(
        self,
        calc_run: "CalcRun",
        dirty_nodes: set[UUID],
        db: DictRowConnection,
    ) -> None:
        if not dirty_nodes:
            return

        params = shortage_params(calc_run.scenario_id, calc_run.calc_run_id)

        # 1. Compute opening/inflows/outflows/closing/has_shortage on dirty PIs.
        cur = db.execute(PROPAGATE_SQL, params)
        rows_updated = cur.rowcount or 0
        logger.debug(
            "SqlPropagationEngine: propagated %d PI nodes (dirty=%d)",
            rows_updated, len(dirty_nodes),
        )
        # The kernel mirrors compute_pi_node — every row in the dirty set has
        # been processed for opening/closing. Per the existing CalcRun contract,
        # accumulate the recalculated/unchanged counters. The SQL UPDATE only
        # writes rows that actually exist, so use rows_updated as the
        # recalculated count and treat the rest as unchanged (the SQL engine
        # does not currently distinguish "changed" vs "unchanged" — every dirty
        # PI is considered recalculated).
        calc_run.nodes_recalculated += rows_updated

        # 2. Safety-stock / negative-closing shortage detection + persistence.
        if self._shortage_detector is not None:
            db.execute(SHORTAGES_SQL, params)

        # 3. Clear dirty flags for the rows we just processed. Use the same
        # contract as DirtyFlagManager.clear_dirty_batch.
        # M3 explainability is regenerated on-demand by /v1/explain (see
        # api/routers/explain.py) — eager regeneration would cost ~5s per
        # 1k shortages and would be wasted for explanations never consumed.
        db.execute(CLEAR_DIRTY_SQL, params)
