"""
DB loading layer for MRP planning data (ADR-020).
Owns the single consolidated DB scan that populates PlanningData.
All SQL lives here; core.py is DB-free.
"""
from __future__ import annotations

import datetime as _dt
import math
from collections import defaultdict

from psycopg.rows import tuple_row

from ootils_core.engine.mrp.core import (
    BASELINE,
    FIRM_RECEIPT_TYPES,
    PlanningData,
    _spread_period,
)
from ootils_core.engine.scenario.param_overlay import resolved_params_sql


def guard_db(dsn: str, allow_dev: bool = False) -> str:
    name = dsn.rstrip("/").split("/")[-1].split("?")[0]
    if not name.startswith("ootils"):
        raise SystemExit(f"REFUSED: DB '{name}' does not start with 'ootils'.")
    if name == "ootils_dev" and not allow_dev:
        raise SystemExit("REFUSED: ootils_dev is semi-prod, pass --allow-dev.")
    return name


def _m(cur, sql, params=None):
    return {r[0]: r[1] for r in cur.execute(sql, params or {}).fetchall()}


def load_planning_data(conn, horizon_days=540, scenario=BASELINE) -> PlanningData:
    # All SQL below reads rows positionally (r[0], r[1], ...). Pin the cursor
    # to tuple rows so this works regardless of the connection's row_factory:
    # the mrp_core CLIs open tuple-row connections, but a scenario-aware caller
    # (watchers, API paths per #347) hands us the app's dict_row connection,
    # under which positional access would raise KeyError: 0.
    cur = conn.cursor(row_factory=tuple_row)
    b = {"b": scenario}
    horizon_start = cur.execute("SELECT CURRENT_DATE").fetchone()[0]
    d = PlanningData(horizon_start=horizon_start, n_buckets=math.ceil(horizon_days / 7) + 1)

    d.llc = _m(cur, "SELECT component_item_id, MAX(llc) FROM bom_lines GROUP BY component_item_id")

    # item_planning_params: ONE scan for all 12 per-item planning aggregates
    # (was 12 separate GROUP BY scans of the same table). Same WHERE + same
    # per-item aggregates => byte-identical dicts. Measured 5.5x / -386ms on the
    # pilote DB (24K params rows); see scripts/bench_mrp.py.
    #
    # Scenario param overlay (ADR-025, chantier #347 PR2): the pooling here is
    # SUM/MAX *across locations* for a single item — an override scoped to one
    # (item, location) must be resolved BEFORE that pooling, or the overlaid
    # value would never reach the aggregate (a location-scoped override on a
    # single-location item is invisible to a bare `GROUP BY item_id` over the
    # raw table; on a multi-location item it would silently pool the UN-
    # overlaid base value instead). resolved_params_sql() is composed as the
    # FROM source of this GROUP BY (joined back to item_planning_params on
    # param_id only for the two fields the resolver does not cover — see
    # below) instead of grouping the raw table directly, so every whitelisted
    # field entering the aggregate is already scenario-resolved.
    # scenario_id=None (baseline) degrades every LEFT JOIN LATERAL inside the
    # fragment to "no override row", producing the byte-identical baseline
    # aggregate — see resolved_params_sql()'s docstring.
    #
    # `order_multiple` (legacy, no `_qty` suffix) and `is_make` are NOT part
    # of ALLOWED_PARAM_FIELDS (mig 060 / ADR-025 V1): is_make would change
    # graph topology (excluded by design from a purely parametric overlay)
    # and order_multiple is the pre-021 legacy column the resolver
    # deliberately does not cover. This core-A loader reads `order_multiple`
    # (the legacy column) straight off base — byte-identical to the pre-PR2
    # `MAX(order_multiple)` — while the APICS engine reads the modern
    # `order_multiple_qty`; that column divergence between the two MRP engines
    # predates #347 and PR2 preserves it exactly. Both `order_multiple` and
    # `is_make` are read straight off the base table here, unresolved, joined
    # back on param_id (the resolver's one row per current item_planning_params
    # row).
    #
    # lead_time_total_days is a GENERATED column on the base table and is NOT
    # in ALLOWED_PARAM_FIELDS either (only its 3 components are overlay-able)
    # — recomputed here as COALESCE(component,0) summed, byte-for-byte the base
    # column's own generation expression (COALESCE(s,0)+COALESCE(m,0)+
    # COALESCE(t,0)), so a NULL component yields the same total as before
    # instead of NULL-propagating.
    resolved_ipp_sql = resolved_params_sql("ipp")
    overlay_scenario_id = scenario if scenario != BASELINE else None
    for r in cur.execute(
        f"""
        SELECT rp.item_id,
            bool_or(base.is_make),
            SUM(COALESCE(rp.safety_stock_qty, 0)),
            MAX(COALESCE(rp.lead_time_sourcing_days, 0)
                + COALESCE(rp.lead_time_manufacturing_days, 0)
                + COALESCE(rp.lead_time_transit_days, 0)),
            MAX(base.order_multiple),
            MIN(rp.lot_size_rule),
            MAX(rp.lot_size_poq_periods),
            MAX(rp.economic_order_qty),
            MAX(rp.max_order_qty),
            MAX(rp.min_order_qty),
            MAX(rp.frozen_time_fence_days),
            MAX(rp.slashed_time_fence_days),
            MIN(rp.forecast_consumption_strategy)
        FROM ({resolved_ipp_sql}) rp
        JOIN item_planning_params base ON base.param_id = rp.param_id
        GROUP BY rp.item_id
        """,
        {"scenario_id": overlay_scenario_id},
    ).fetchall():
        it = r[0]
        d.is_make[it] = r[1]
        d.safety[it] = r[2]
        d.make_lt[it] = r[3]
        d.mult[it] = r[4]
        d.lot_rule[it] = r[5]
        d.poq_per[it] = r[6]
        d.eoq[it] = r[7]
        d.max_oq[it] = r[8]
        d.min_oq[it] = r[9]
        d.frozen_d[it] = r[10]
        d.slushy_d[it] = r[11]
        d.strat[it] = r[12]

    # nodes: on-hand + firm receipts in ONE scenario-scoped scan (was 2), via
    # FILTER. MIN/SUM-over-empty => NULL, skipped, so dict keys match the old
    # per-type queries exactly. Scenario-scoped (scenario MRP reads its fork).
    for r in cur.execute(
        "SELECT item_id, "
        "SUM(quantity) FILTER (WHERE node_type='OnHandSupply'), "
        "SUM(quantity) FILTER (WHERE node_type=ANY(%(firm)s)) "
        "FROM nodes WHERE scenario_id=%(b)s AND active "
        "AND node_type=ANY(%(all)s) GROUP BY item_id",
        {"b": scenario, "firm": FIRM_RECEIPT_TYPES,
         "all": ["OnHandSupply", *FIRM_RECEIPT_TYPES]},
    ).fetchall():
        if r[1] is not None:
            d.on_hand[r[0]] = r[1]
        if r[2] is not None:
            d.firm[r[0]] = r[2]

    # supplier_items: buy lead time + MOQ in ONE scan (MIN ignores NULLs, so the
    # old "WHERE col IS NOT NULL" filters reduce to skipping NULL results).
    for r in cur.execute(
        "SELECT item_id, MIN(lead_time_days), MIN(moq) "
        "FROM supplier_items GROUP BY item_id"
    ).fetchall():
        if r[1] is not None:
            d.buy_lt[r[0]] = r[1]
        if r[2] is not None:
            d.moq[r[0]] = r[2]

    # items: external id + standard cost/currency in ONE scan (was 3). std_cost
    # and std_ccy stay keyed only on priced items (matches the old WHERE filter).
    for r in cur.execute(
        "SELECT item_id, external_id, standard_cost, cost_currency FROM items"
    ).fetchall():
        d.names[r[0]] = r[1]
        if r[2] is not None:
            d.std_cost[r[0]] = r[2]
            d.std_ccy[r[0]] = r[3]

    d.sup_name = _m(cur, "SELECT external_id, name FROM suppliers WHERE external_id IS NOT NULL")

    for parent, comp, qpb, scrap in cur.execute(
        "SELECT bh.parent_item_id, bl.component_item_id, bl.quantity_per, bl.scrap_factor "
        "FROM bom_headers bh JOIN bom_lines bl ON bl.bom_id=bh.bom_id "
        "WHERE bh.effective_to IS NULL OR bh.effective_to > CURRENT_DATE").fetchall():
        d.bom.setdefault(parent, []).append((comp, float(qpb), float(scrap or 0)))

    # make/buy resolution: any item with an active BOM is manufactured (make),
    # even when the is_make planning flag is missing. A missing flag must not
    # silently turn a manufactured parent into a phantom (uncosted) purchase —
    # it would explode the purchase plan with buy orders that have no supplier.
    for parent in d.bom:
        d.is_make[parent] = True

    for item, sid, sext, lt, uc, ccy, rel in cur.execute(
        "SELECT DISTINCT ON (si.item_id) si.item_id, s.supplier_id, s.external_id, "
        "si.lead_time_days, si.unit_cost, si.currency, s.reliability_score "
        "FROM supplier_items si JOIN suppliers s ON s.supplier_id=si.supplier_id "
        "WHERE si.lead_time_days IS NOT NULL "
        # cost-aware pick: prefer the preferred supplier, but among ties take a row
        # that actually carries a unit_cost before falling back to shortest lead time
        "ORDER BY si.item_id, si.is_preferred DESC, (si.unit_cost IS NULL), si.lead_time_days ASC").fetchall():
        d.best_sup[item] = (sid, sext, lt, uc, ccy, rel)

    # dedicated cost map: a representative unit_cost from ANY priced supplier row
    # (decoupled from supplier identity / lead-time filter) so valuation isn't
    # starved when the chosen supplier happens to carry no cost.
    for item, uc, ccy in cur.execute(
        "SELECT DISTINCT ON (item_id) item_id, unit_cost, currency FROM supplier_items "
        "WHERE unit_cost IS NOT NULL AND unit_cost > 0 "
        "ORDER BY item_id, is_preferred DESC, unit_cost ASC").fetchall():
        d.unit_cost[item] = float(uc)
        d.cost_ccy[item] = ccy or "USD"

    d.co_b = defaultdict(lambda: defaultdict(float))
    for item, tref, qty in cur.execute(
        "SELECT item_id, time_ref, quantity FROM nodes WHERE scenario_id=%(b)s AND active "
        "AND node_type='CustomerOrderDemand' AND time_ref IS NOT NULL AND quantity IS NOT NULL", b).fetchall():
        if tref >= horizon_start:
            d.co_b[item][d.bucket(tref)] += float(qty)
    # Forecast: prorate each line across the weekly buckets of the period it
    # covers. A line's period runs from its date to the NEXT forecast date for
    # the same item (inferred granularity — monthly/quarterly/weekly); the qty is
    # spread proportional to day-overlap. Already-weekly forecasts => no-op.
    # Aggregate forecast by (item, date) FIRST: there can be several forecast
    # nodes for the same item+date (one per location). Summing them = item-level
    # pooled demand, and — critically — collapses duplicate dates so the
    # period-to-next-date proration below never sees a zero-length period (which
    # would silently drop a location's volume).
    raw_fc = defaultdict(lambda: defaultdict(float))
    for item, tref, qty in cur.execute(
        "SELECT item_id, time_ref, quantity FROM nodes WHERE scenario_id=%(b)s AND active "
        "AND node_type='ForecastDemand' AND time_ref IS NOT NULL AND quantity IS NOT NULL", b).fetchall():
        raw_fc[item][tref] += float(qty)
    horizon_end = horizon_start + _dt.timedelta(days=horizon_days)
    d.fc_b = defaultdict(lambda: defaultdict(float))
    for item, datemap in raw_fc.items():
        rows = sorted(datemap.items())
        gaps = [(rows[i + 1][0] - rows[i][0]).days for i in range(len(rows) - 1)
                if (rows[i + 1][0] - rows[i][0]).days > 0]
        default_span = sorted(gaps)[len(gaps) // 2] if gaps else 7  # median gap, else weekly
        for i, (tref, qty) in enumerate(rows):
            end = rows[i + 1][0] if i + 1 < len(rows) else tref + _dt.timedelta(days=default_span)
            _spread_period(qty, tref, end, horizon_start, horizon_end, d.n_buckets, d.fc_b[item])
    d.sched_b = defaultdict(lambda: defaultdict(float))
    for item, tref, qty in cur.execute(
        "SELECT item_id, time_ref, quantity FROM nodes WHERE scenario_id=%(b)s AND active "
        "AND node_type=ANY(%(t)s) AND time_ref IS NOT NULL AND quantity IS NOT NULL",
        {"b": scenario, "t": FIRM_RECEIPT_TYPES}).fetchall():
        d.sched_b[item][d.bucket(tref)] += float(qty)

    involved = set()
    for m in (d.llc, d.is_make, d.on_hand, d.safety, d.co_b, d.fc_b, d.sched_b):
        involved.update(m.keys())
    for parent, comps in d.bom.items():
        involved.add(parent)
        for c, _, _ in comps:
            involved.add(c)
    d.involved = involved
    d.max_llc = max((d.llc.get(i, 0) for i in involved), default=0)
    d.by_level = defaultdict(list)
    for i in involved:
        d.by_level[d.llc.get(i, 0)].append(i)
    return d
