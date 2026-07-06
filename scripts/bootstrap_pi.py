"""
bootstrap_pi.py — Materialize the ProjectedInventory graph (PI buckets + edges).

After bulk_ingest.py loads supply / demand nodes, this script creates the
ProjectedInventory time-series infrastructure that the engine needs to compute
projections and detect shortages.

For each (item, location) pair that has activity (at least one node), it
creates:
  1. one projection_series row
  2. N daily ProjectedInventory nodes (horizon)
  3. feeds_forward edges PI[t] → PI[t+1] within each series
  4. replenishes / consumes edges from existing supply/demand nodes to the
     matching PI bucket by date

Scenario-first (#414): every derivation query AND every node/edge INSERT is
scoped to --scenario (default: baseline, so the historical CLI is unchanged).
On the pilote base the intended path is a dedicated fork: bootstrap INTO the
fork on a coherent BOM subset over a short horizon, never a 36k-item baseline
big-bang (13-20M nodes = permanent debt). The 2M-node volumetric guard below is
the anti-big-bang rampart.

Subset selection (two mutually exclusive flags; neither ⇒ full scope, unchanged):
  --sample-finished N : the N FINISHED items (never a component of any active
      BOM = LLC 0) with the strongest booking demand (ordered_quantity over the
      last 365 days, summed per item — warehouse_id is not reliably mapped to a
      location on the pilote, so demand is item-level), PLUS the full BOM
      sub-tree closure of each (we never project a parent without its
      components — LLC coherence).
  --items-file PATH   : explicit external_id list (one per line), PLUS the same
      BOM sub-tree closure.

Usage:
    python scripts/bootstrap_pi.py --horizon-days 540 --sample 1000
    python scripts/bootstrap_pi.py --horizon-days 540  # full scope (all active pairs)
    python scripts/bootstrap_pi.py --scenario <fork-uuid> --sample-finished 300 --horizon-days 120

Safety: only writes to DB whose name starts with 'ootils_' and is not 'ootils_dev'
unless --allow-dev is given.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from uuid import UUID

import psycopg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger("bootstrap_pi")

BASELINE_SCENARIO_ID = "00000000-0000-0000-0000-000000000001"

# Anti-big-bang rampart (#414): projecting the whole 36k-item pilote baseline
# would materialise 13-20M PI nodes — permanent debt the propagator then has to
# carry on every run. Refuse above this unless --force. See docs/SCALABILITY.md.
MAX_PROJECTED_NODES = 2_000_000

# Demand ranking window for --sample-finished (booking-based, per the demand
# business rule: forecast on booking, never shipping).
_DEMAND_WINDOW_DAYS = 365


def _guard_db(dsn: str, allow_dev: bool) -> str:
    db_name = dsn.rstrip("/").split("/")[-1].split("?")[0]
    if not db_name.startswith("ootils"):
        raise SystemExit(f"REFUSED: DB '{db_name}' does not start with 'ootils'.")
    if db_name == "ootils_dev" and not allow_dev:
        raise SystemExit(f"REFUSED: '{db_name}' is semi-prod, pass --allow-dev to override.")
    return db_name


def _count_scenario_nodes(cur: psycopg.Cursor, scenario_id: str) -> int:
    cur.execute(
        "SELECT COUNT(*) FROM nodes WHERE scenario_id = %s::uuid",
        (scenario_id,),
    )
    row = cur.fetchone()
    return int(row[0]) if row else 0


def _read_external_ids(path: Path) -> list[str]:
    """One external_id per line; blanks and '#'-comment lines ignored."""
    out: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if s and not s.startswith("#"):
            out.append(s)
    return out


def _build_seed_items(
    cur: psycopg.Cursor,
    *,
    sample_finished: int | None,
    external_ids: list[str] | None,
) -> None:
    """Materialise _b_seed_items(item_id) — the chosen seed BEFORE BOM closure.

    Exactly one of sample_finished / external_ids is set here (the CLI enforces
    mutual exclusivity). Finished = an item that is never a component of any
    active BOM line (LLC 0: BOM roots + standalone items).
    """
    cur.execute("CREATE TEMP TABLE _b_seed_items (item_id UUID PRIMARY KEY) ON COMMIT DROP")

    if external_ids is not None:
        cur.execute(
            """
            INSERT INTO _b_seed_items (item_id)
            SELECT DISTINCT it.item_id
            FROM items it
            JOIN UNNEST(%s::text[]) AS req(external_id) ON req.external_id = it.external_id
            ON CONFLICT DO NOTHING
            """,
            (external_ids,),
        )
        return

    # sample_finished: FINISHED items (never a BOM component) ranked by booking
    # demand over the last _DEMAND_WINDOW_DAYS, summed per item (warehouse_id is
    # not reliably mapped to a location on the pilote — item-level is the honest
    # aggregation). Items with no demand still qualify (demand 0, sorted last)
    # so a coherent subset is always producible even on a demand-less base.
    window_start = date.today() - timedelta(days=_DEMAND_WINDOW_DAYS)
    cur.execute(
        """
        INSERT INTO _b_seed_items (item_id)
        SELECT it.item_id
        FROM items it
        LEFT JOIN LATERAL (
            SELECT COALESCE(SUM(dh.ordered_quantity), 0) AS demand
            FROM demand_history dh
            WHERE dh.item_id = it.item_id
              AND dh.stream = 'regular'
              AND dh.booked_date IS NOT NULL
              AND dh.booked_date >= %s::date
        ) d ON TRUE
        WHERE it.status = 'active'
          AND it.item_id NOT IN (
              SELECT bl.component_item_id
              FROM bom_lines bl
              JOIN bom_headers bh ON bh.bom_id = bl.bom_id
              WHERE bh.status = 'active' AND bl.active = TRUE
          )
        ORDER BY d.demand DESC, it.external_id
        LIMIT %s
        """,
        (window_start, sample_finished),
    )


def _close_bom_subtree(cur: psycopg.Cursor) -> None:
    """Expand _b_seed_items into _b_scope_items via the full active-BOM closure.

    A recursive walk parent → component over active bom_headers/bom_lines. The
    seed items are the roots; every transitive component joins the scope so we
    never project a parent without the components its dependent demand needs.
    """
    cur.execute(
        """
        CREATE TEMP TABLE _b_scope_items (item_id UUID PRIMARY KEY) ON COMMIT DROP
        """
    )
    cur.execute(
        """
        WITH RECURSIVE subtree(item_id) AS (
            SELECT item_id FROM _b_seed_items
            UNION
            SELECT bl.component_item_id
            FROM subtree s
            JOIN bom_headers bh ON bh.parent_item_id = s.item_id AND bh.status = 'active'
            JOIN bom_lines bl ON bl.bom_id = bh.bom_id AND bl.active = TRUE
        )
        INSERT INTO _b_scope_items (item_id)
        SELECT item_id FROM subtree
        ON CONFLICT DO NOTHING
        """
    )


def bootstrap(
    conn: psycopg.Connection,
    horizon: int,
    sample: int | None,
    *,
    scenario_id: str = BASELINE_SCENARIO_ID,
    sample_finished: int | None = None,
    items_file: Path | None = None,
    force: bool = False,
) -> dict:
    """Bootstrap the PI graph for one scenario.

    Returns a dict with counts and per-phase timings.
    """
    cur = conn.cursor()
    today = date.today()
    horizon_start = today
    horizon_end = today + timedelta(days=horizon)
    timings: dict[str, float] = {}

    nodes_before = _count_scenario_nodes(cur, scenario_id)

    subset_mode = "full"
    external_ids: list[str] | None = None
    if items_file is not None:
        external_ids = _read_external_ids(items_file)
        subset_mode = "items_file"
    elif sample_finished is not None:
        subset_mode = "sample_finished"

    # ── 0. Optional subset: seed items → BOM sub-tree closure ─────
    t0 = time.perf_counter()
    scope_join = ""
    if subset_mode != "full":
        _build_seed_items(
            cur, sample_finished=sample_finished, external_ids=external_ids
        )
        _close_bom_subtree(cur)
        cur.execute("SELECT COUNT(*) FROM _b_seed_items")
        seed_row = cur.fetchone()
        n_seed_items = int(seed_row[0]) if seed_row else 0
        cur.execute("SELECT COUNT(*) FROM _b_scope_items")
        scope_row = cur.fetchone()
        n_scope_items = int(scope_row[0]) if scope_row else 0
        # Restrict the pair-derivation to items in the closed scope.
        scope_join = "JOIN _b_scope_items si ON si.item_id = nodes.item_id"
        logger.info(
            "Subset %s: %d seed item(s) → %d item(s) after BOM closure",
            subset_mode,
            n_seed_items,
            n_scope_items,
        )
    else:
        n_seed_items = 0
        n_scope_items = 0
    timings["0_subset_s"] = round(time.perf_counter() - t0, 2)

    # ── 1. Identify (item, location) pairs with activity ──────────
    t0 = time.perf_counter()
    cur.execute(
        f"""
        CREATE TEMP TABLE _b_pi_pairs AS
        SELECT DISTINCT nodes.item_id, nodes.location_id
        FROM nodes
        {scope_join}
        WHERE nodes.scenario_id = %s::uuid
          AND nodes.item_id IS NOT NULL
          AND nodes.location_id IS NOT NULL
          AND nodes.node_type IN ('OnHandSupply', 'PurchaseOrderSupply', 'TransferSupply',
                            'CustomerOrderDemand', 'ForecastDemand')
        """,
        (scenario_id,),
    )
    cur.execute("SELECT COUNT(*) FROM _b_pi_pairs")
    total_pairs_row = cur.fetchone()
    total_pairs = int(total_pairs_row[0]) if total_pairs_row else 0
    logger.info("Found %d (item, location) pairs with activity", total_pairs)

    # Random sampling (legacy --sample) is applied AFTER subset selection.
    if sample is not None and sample < total_pairs:
        cur.execute(
            "CREATE TEMP TABLE _b_pi_sample AS SELECT * FROM _b_pi_pairs ORDER BY random() LIMIT %s",
            (sample,),
        )
        pairs_table = "_b_pi_sample"
        logger.info("Sampling %d pairs", sample)
    else:
        pairs_table = "_b_pi_pairs"
    cur.execute(f"SELECT COUNT(*) FROM {pairs_table}")
    n_pairs_row = cur.fetchone()
    n_pairs = int(n_pairs_row[0]) if n_pairs_row else 0
    timings["1_identify_pairs_s"] = round(time.perf_counter() - t0, 2)

    # ── 1b. Volumetric guard (anti-big-bang) ──────────────────────
    projected_nodes = n_pairs * horizon
    if projected_nodes > MAX_PROJECTED_NODES and not force:
        raise SystemExit(
            f"REFUSED: {n_pairs} pair(s) × {horizon}-day horizon would materialise "
            f"~{projected_nodes:,} ProjectedInventory nodes, above the "
            f"{MAX_PROJECTED_NODES:,} safety ceiling. Narrow the subset "
            "(--sample-finished / --items-file), shorten --horizon-days, or pass "
            "--force to override (accepts the volumetric debt knowingly)."
        )

    # ── 2. Create projection_series ───────────────────────────────
    t0 = time.perf_counter()
    cur.execute(
        f"""
        INSERT INTO projection_series (item_id, location_id, scenario_id, horizon_start, horizon_end)
        SELECT p.item_id, p.location_id, %s::uuid, %s::date, %s::date
        FROM {pairs_table} p
        ON CONFLICT (item_id, location_id, scenario_id) DO NOTHING
        """,
        (scenario_id, horizon_start, horizon_end),
    )
    n_series = cur.rowcount
    timings["2_create_series_s"] = round(time.perf_counter() - t0, 2)
    logger.info("Created %d projection_series in %.2fs", n_series, timings["2_create_series_s"])

    # ── 3. Create PI nodes (one per day per series) ───────────────
    t0 = time.perf_counter()
    cur.execute(
        f"""
        INSERT INTO nodes (
            node_type, scenario_id, item_id, location_id,
            time_grain, time_ref, time_span_start, time_span_end,
            projection_series_id, bucket_sequence,
            opening_stock, inflows, outflows, closing_stock,
            active
        )
        SELECT
            'ProjectedInventory',
            %s::uuid,
            ps.item_id,
            ps.location_id,
            'day',
            (%s::date + (gs.day_offset * INTERVAL '1 day'))::date,
            (%s::date + (gs.day_offset * INTERVAL '1 day'))::date,
            (%s::date + ((gs.day_offset + 1) * INTERVAL '1 day'))::date,
            ps.series_id,
            gs.day_offset,
            0, 0, 0, 0,
            TRUE
        FROM projection_series ps
        JOIN {pairs_table} p ON p.item_id = ps.item_id AND p.location_id = ps.location_id
        CROSS JOIN generate_series(0, %s - 1) AS gs(day_offset)
        WHERE ps.scenario_id = %s::uuid
        """,
        (scenario_id, horizon_start, horizon_start, horizon_start, horizon, scenario_id),
    )
    n_pi = cur.rowcount
    timings["3_create_pi_nodes_s"] = round(time.perf_counter() - t0, 2)
    logger.info("Created %d ProjectedInventory nodes in %.2fs", n_pi, timings["3_create_pi_nodes_s"])

    # ── 4. feeds_forward edges PI[t] → PI[t+1] within each series ─
    # Use LEAD window function instead of self-join → single sort+scan, much faster
    t0 = time.perf_counter()
    cur.execute(
        """
        INSERT INTO edges (edge_type, from_node_id, to_node_id, scenario_id, active)
        SELECT 'feeds_forward', node_id, next_node_id, %s::uuid, TRUE
        FROM (
            SELECT
                node_id,
                LEAD(node_id) OVER (PARTITION BY projection_series_id ORDER BY bucket_sequence) AS next_node_id
            FROM nodes
            WHERE node_type = 'ProjectedInventory'
              AND scenario_id = %s::uuid
              AND projection_series_id IS NOT NULL
        ) sub
        WHERE next_node_id IS NOT NULL
        """,
        (scenario_id, scenario_id),
    )
    n_ff = cur.rowcount
    timings["4_feeds_forward_s"] = round(time.perf_counter() - t0, 2)
    logger.info("Created %d feeds_forward edges in %.2fs", n_ff, timings["4_feeds_forward_s"])

    # ── 5. Wire supply nodes (OnHand, PO, Transfer) → PI buckets ──
    # Each supply node points to the PI bucket with matching date.
    t0 = time.perf_counter()
    cur.execute(
        f"""
        INSERT INTO edges (edge_type, from_node_id, to_node_id, scenario_id, weight_ratio, active)
        SELECT 'replenishes', supply.node_id, pi.node_id, %s::uuid, 1.0, TRUE
        FROM nodes supply
        JOIN {pairs_table} p ON p.item_id = supply.item_id AND p.location_id = supply.location_id
        JOIN nodes pi ON pi.projection_series_id IS NOT NULL
                     AND pi.item_id = supply.item_id
                     AND pi.location_id = supply.location_id
                     AND pi.node_type = 'ProjectedInventory'
                     AND pi.scenario_id = %s::uuid
                     AND pi.time_span_start = supply.time_ref
        WHERE supply.node_type IN ('OnHandSupply', 'PurchaseOrderSupply', 'TransferSupply')
          AND supply.scenario_id = %s::uuid
          AND supply.active = TRUE
          AND supply.time_ref BETWEEN %s::date AND %s::date
        """,
        (scenario_id, scenario_id, scenario_id, horizon_start, horizon_end),
    )
    n_sup_edges = cur.rowcount
    timings["5_supply_edges_s"] = round(time.perf_counter() - t0, 2)
    logger.info("Created %d supply→PI edges in %.2fs", n_sup_edges, timings["5_supply_edges_s"])

    # ── 6. Wire demand nodes (CO, ForecastDemand) → PI buckets ────
    t0 = time.perf_counter()
    cur.execute(
        f"""
        INSERT INTO edges (edge_type, from_node_id, to_node_id, scenario_id, weight_ratio, active)
        SELECT 'consumes', demand.node_id, pi.node_id, %s::uuid, 1.0, TRUE
        FROM nodes demand
        JOIN {pairs_table} p ON p.item_id = demand.item_id AND p.location_id = demand.location_id
        JOIN nodes pi ON pi.projection_series_id IS NOT NULL
                     AND pi.item_id = demand.item_id
                     AND pi.location_id = demand.location_id
                     AND pi.node_type = 'ProjectedInventory'
                     AND pi.scenario_id = %s::uuid
                     AND pi.time_span_start = demand.time_ref
        WHERE demand.node_type IN ('CustomerOrderDemand', 'ForecastDemand')
          AND demand.scenario_id = %s::uuid
          AND demand.active = TRUE
          AND demand.time_ref BETWEEN %s::date AND %s::date
        """,
        (scenario_id, scenario_id, scenario_id, horizon_start, horizon_end),
    )
    n_dem_edges = cur.rowcount
    timings["6_demand_edges_s"] = round(time.perf_counter() - t0, 2)
    logger.info("Created %d demand→PI edges in %.2fs", n_dem_edges, timings["6_demand_edges_s"])

    nodes_after = _count_scenario_nodes(cur, scenario_id)

    return {
        "scenario_id": scenario_id,
        "subset_mode": subset_mode,
        "seed_items": n_seed_items,
        "scope_items_after_bom_closure": n_scope_items,
        "pairs_in_scope": n_pairs,
        "total_pairs_with_activity": total_pairs,
        "projected_nodes_estimate": projected_nodes,
        "volumetric_ceiling": MAX_PROJECTED_NODES,
        "forced": force,
        "horizon_days": horizon,
        "horizon_start": str(horizon_start),
        "horizon_end": str(horizon_end),
        "projection_series_created": n_series,
        "pi_nodes_created": n_pi,
        "feeds_forward_edges": n_ff,
        "supply_edges": n_sup_edges,
        "demand_edges": n_dem_edges,
        "edges_created": n_ff + n_sup_edges + n_dem_edges,
        "total_rows": n_series + n_pi + n_ff + n_sup_edges + n_dem_edges,
        "scenario_nodes_before": nodes_before,
        "scenario_nodes_after": nodes_after,
        "timings_s": timings,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Bootstrap ProjectedInventory graph after bulk_ingest.")
    parser.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    parser.add_argument(
        "--scenario",
        default=BASELINE_SCENARIO_ID,
        help="Target scenario UUID (default: baseline). Derivation AND inserts are scoped to it.",
    )
    parser.add_argument("--horizon-days", "--horizon", type=int, default=540,
                        dest="horizon_days",
                        help="Horizon in days (default 540 = 18 months)")
    parser.add_argument("--sample", type=int, default=None,
                        help="Random-sample N (item, location) pairs from the scope (default: full scope)")
    parser.add_argument("--sample-finished", type=int, default=None,
                        help="Seed with the N highest-demand FINISHED items (+ their BOM sub-tree)")
    parser.add_argument("--items-file", type=Path, default=None,
                        help="Seed with explicit external_ids from a file (+ their BOM sub-tree)")
    parser.add_argument("--force", action="store_true",
                        help="Override the 2M-node volumetric guard (accept the debt knowingly)")
    parser.add_argument("--allow-dev", action="store_true")
    args = parser.parse_args(argv)

    if not args.dsn:
        logger.error("DATABASE_URL not set and --dsn not provided")
        return 2

    if args.sample_finished is not None and args.items_file is not None:
        logger.error("--sample-finished and --items-file are mutually exclusive")
        return 2

    try:
        UUID(args.scenario)
    except ValueError:
        logger.error("--scenario must be a valid UUID, got %r", args.scenario)
        return 2

    db = _guard_db(args.dsn, args.allow_dev)
    logger.info(
        "Bootstrap PI: DB=%s scenario=%s horizon=%dj sample=%s sample_finished=%s items_file=%s",
        db, args.scenario, args.horizon_days, args.sample,
        args.sample_finished, args.items_file,
    )

    t0 = time.perf_counter()
    with psycopg.connect(args.dsn) as conn:
        result = bootstrap(
            conn,
            args.horizon_days,
            args.sample,
            scenario_id=args.scenario,
            sample_finished=args.sample_finished,
            items_file=args.items_file,
            force=args.force,
        )
        conn.commit()
    total = round(time.perf_counter() - t0, 2)
    result["wall_total_s"] = total

    logger.info("=" * 60)
    logger.info("PI BOOTSTRAP DONE in %.2fs", total)
    logger.info("  Scenario                : %s", result["scenario_id"])
    logger.info("  Subset mode             : %s", result["subset_mode"])
    if result["subset_mode"] != "full":
        logger.info("  Seed items              : %d", result["seed_items"])
        logger.info("  After BOM closure       : %d", result["scope_items_after_bom_closure"])
    logger.info("  Pairs in scope          : %d / %d", result["pairs_in_scope"], result["total_pairs_with_activity"])
    logger.info("  Horizon                 : %s → %s (%d days)", result["horizon_start"], result["horizon_end"], result["horizon_days"])
    logger.info("  projection_series       : %d", result["projection_series_created"])
    logger.info("  ProjectedInventory nodes: %d", result["pi_nodes_created"])
    logger.info("  feeds_forward edges     : %d", result["feeds_forward_edges"])
    logger.info("  supply→PI edges         : %d", result["supply_edges"])
    logger.info("  demand→PI edges         : %d", result["demand_edges"])
    logger.info("  Scenario nodes          : %d → %d", result["scenario_nodes_before"], result["scenario_nodes_after"])
    logger.info("  TOTAL ROWS              : %d", result["total_rows"])
    logger.info("=" * 60)

    # Machine-readable metrics on stdout, one clearly-marked line (#414 C).
    print("BOOTSTRAP_METRICS: " + json.dumps(result, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
