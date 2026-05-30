"""
mrp_core.py — shared MRP primitives used by the watcher agents and CLIs.

Single source of truth for: planning-data loading, forecast consumption +
demand time fence, the LLC level-by-level time-phased cascade with lot sizing
and lead-time offsetting, and pegging (origin attribution). Extracted so the
demand-side correctness (no CO+forecast double-counting) lives in ONE place and
every consumer inherits it.

Consumers: mrp_timephased.py, mrp_run.py, mrp_peg.py, agent_material_watcher.py,
shortage_scan.py.
"""
from __future__ import annotations

import datetime as _dt
import math
from collections import defaultdict
from dataclasses import dataclass, field

BASELINE = "00000000-0000-0000-0000-000000000001"
DEFAULT_LT_DAYS = 30
SUPPLY_TYPES = ["OnHandSupply", "PurchaseOrderSupply", "TransferSupply"]
FIRM_RECEIPT_TYPES = ["PurchaseOrderSupply", "WorkOrderSupply", "TransferSupply"]
DEMAND_TYPES = ["CustomerOrderDemand", "ForecastDemand"]


def guard_db(dsn: str, allow_dev: bool = False) -> str:
    name = dsn.rstrip("/").split("/")[-1].split("?")[0]
    if not name.startswith("ootils"):
        raise SystemExit(f"REFUSED: DB '{name}' does not start with 'ootils'.")
    if name == "ootils_dev" and not allow_dev:
        raise SystemExit("REFUSED: ootils_dev is semi-prod, pass --allow-dev.")
    return name


def _m(cur, sql, params=None):
    return {r[0]: r[1] for r in cur.execute(sql, params or {}).fetchall()}


def lot_size(qty, moq, mult):
    """MOQ floor + order-multiple rounding (final guard applied to every rule)."""
    if moq and qty < moq:
        qty = moq
    if mult and mult > 0:
        qty = math.ceil(qty / mult) * mult
    return qty


def cost_of(d, item):
    """Unit cost + currency for an item, single source of the valuation precedence:
    negotiated supplier unit_cost first, then item standard_cost. Returns
    (None, None) when unpriced. A loaded cost of 0 is a real price, not 'missing' —
    only None means unpriced (avoids the `unit_cost or std_cost` truthiness trap).
    """
    uc = d.unit_cost.get(item)
    ccy = d.cost_ccy.get(item)
    if uc is None:
        uc = d.std_cost.get(item)
        ccy = d.std_ccy.get(item)
    return (float(uc), ccy or "USD") if uc is not None else (None, None)


def _spread_period(qty, start, end, horizon_start, horizon_end, n_buckets, out):
    """Prorate qty across weekly buckets proportional to the days each bucket
    overlaps the period [start, end). Mass-conserving when the period is inside
    the horizon; the fraction falling outside the horizon is dropped (we only
    plan forward). Used to spread lumpy (e.g. monthly) forecasts into weeks.
    """
    span = (end - start).days
    if span <= 0:
        return
    day = max(start, horizon_start)
    last = min(end, horizon_end)
    while day < last:
        bk = max(0, (day - horizon_start).days // 7)
        bk_end = horizon_start + _dt.timedelta(days=(bk + 1) * 7)
        seg_end = min(last, bk_end)
        days = (seg_end - day).days
        if days > 0 and bk < n_buckets:
            out[bk] += qty * days / span
        day = seg_end


def apply_lot_rule(rule, shortfall, pa, ss, netreq, t, P, eoq, maxoq, moq, mult, n_buckets):
    """Compute order qty for a shortfall at bucket t under a lot-sizing rule."""
    if rule == "POQ":
        window = sum(max(0.0, netreq.get(k, 0.0)) for k in range(t + 1, min(t + P, n_buckets)))
        qty = shortfall + window
    elif rule == "EOQ" and eoq > 0:
        qty = max(eoq, shortfall)
    elif rule == "MIN_MAX" and maxoq > 0:
        qty = (ss + maxoq) - pa
    elif rule == "FIXED_QTY" and moq > 0:
        qty = math.ceil(shortfall / moq) * moq
    else:  # LOTFORLOT / MULTIPLE / fallback
        qty = shortfall
    q = lot_size(qty, moq, mult)
    # max_order_qty is a per-order ceiling. MIN_MAX already uses maxoq as a target
    # stock level (ss+maxoq), so don't re-cap that rule with the same field. For
    # every other rule, never exceed the supplier's max order quantity.
    if maxoq and maxoq > 0 and rule != "MIN_MAX" and q > maxoq:
        q = float(maxoq)
    return q


@dataclass
class PlanningData:
    horizon_start: _dt.date
    n_buckets: int
    llc: dict = field(default_factory=dict)
    is_make: dict = field(default_factory=dict)
    on_hand: dict = field(default_factory=dict)
    firm: dict = field(default_factory=dict)
    safety: dict = field(default_factory=dict)
    buy_lt: dict = field(default_factory=dict)
    make_lt: dict = field(default_factory=dict)
    moq: dict = field(default_factory=dict)
    mult: dict = field(default_factory=dict)
    lot_rule: dict = field(default_factory=dict)
    poq_per: dict = field(default_factory=dict)
    eoq: dict = field(default_factory=dict)
    max_oq: dict = field(default_factory=dict)
    min_oq: dict = field(default_factory=dict)
    frozen_d: dict = field(default_factory=dict)
    slushy_d: dict = field(default_factory=dict)
    strat: dict = field(default_factory=dict)
    names: dict = field(default_factory=dict)
    bom: dict = field(default_factory=dict)
    best_sup: dict = field(default_factory=dict)
    unit_cost: dict = field(default_factory=dict)
    cost_ccy: dict = field(default_factory=dict)
    std_cost: dict = field(default_factory=dict)
    std_ccy: dict = field(default_factory=dict)
    sup_name: dict = field(default_factory=dict)
    co_b: dict = field(default_factory=dict)
    fc_b: dict = field(default_factory=dict)
    sched_b: dict = field(default_factory=dict)
    involved: set = field(default_factory=set)
    by_level: dict = field(default_factory=dict)
    max_llc: int = 0

    def bucket(self, d):
        return max(0, (d - self.horizon_start).days // 7)

    def lt_weeks(self, item):
        make = bool(self.is_make.get(item, False))
        lt = (self.make_lt.get(item) if make else self.buy_lt.get(item)) or DEFAULT_LT_DAYS
        return max(0, math.ceil(float(lt) / 7))


def load_planning_data(conn, horizon_days=540, scenario=BASELINE) -> PlanningData:
    cur = conn.cursor()
    b = {"b": scenario}
    horizon_start = cur.execute("SELECT CURRENT_DATE").fetchone()[0]
    d = PlanningData(horizon_start=horizon_start, n_buckets=math.ceil(horizon_days / 7) + 1)

    d.llc = _m(cur, "SELECT component_item_id, MAX(llc) FROM bom_lines GROUP BY component_item_id")
    d.is_make = _m(cur, "SELECT item_id, bool_or(is_make) FROM item_planning_params WHERE effective_to IS NULL GROUP BY item_id")
    d.on_hand = _m(cur, "SELECT item_id, SUM(quantity) FROM nodes WHERE scenario_id=%(b)s AND active AND node_type='OnHandSupply' GROUP BY item_id", b)
    d.firm = _m(cur, "SELECT item_id, SUM(quantity) FROM nodes WHERE scenario_id=%(b)s AND active AND node_type=ANY(%(t)s) GROUP BY item_id", {"b": scenario, "t": FIRM_RECEIPT_TYPES})
    d.safety = _m(cur, "SELECT item_id, SUM(COALESCE(safety_stock_qty,0)) FROM item_planning_params WHERE effective_to IS NULL GROUP BY item_id")
    d.make_lt = _m(cur, "SELECT item_id, MAX(lead_time_total_days) FROM item_planning_params WHERE effective_to IS NULL GROUP BY item_id")
    d.buy_lt = _m(cur, "SELECT item_id, MIN(lead_time_days) FROM supplier_items WHERE lead_time_days IS NOT NULL GROUP BY item_id")
    d.moq = _m(cur, "SELECT item_id, MIN(moq) FROM supplier_items WHERE moq IS NOT NULL GROUP BY item_id")
    d.mult = _m(cur, "SELECT item_id, MAX(order_multiple) FROM item_planning_params WHERE effective_to IS NULL GROUP BY item_id")
    d.lot_rule = _m(cur, "SELECT item_id, MIN(lot_size_rule::text) FROM item_planning_params WHERE effective_to IS NULL GROUP BY item_id")
    d.poq_per = _m(cur, "SELECT item_id, MAX(lot_size_poq_periods) FROM item_planning_params WHERE effective_to IS NULL GROUP BY item_id")
    d.eoq = _m(cur, "SELECT item_id, MAX(economic_order_qty) FROM item_planning_params WHERE effective_to IS NULL GROUP BY item_id")
    d.max_oq = _m(cur, "SELECT item_id, MAX(max_order_qty) FROM item_planning_params WHERE effective_to IS NULL GROUP BY item_id")
    d.min_oq = _m(cur, "SELECT item_id, MAX(min_order_qty) FROM item_planning_params WHERE effective_to IS NULL GROUP BY item_id")
    d.frozen_d = _m(cur, "SELECT item_id, MAX(frozen_time_fence_days) FROM item_planning_params WHERE effective_to IS NULL GROUP BY item_id")
    d.slushy_d = _m(cur, "SELECT item_id, MAX(slashed_time_fence_days) FROM item_planning_params WHERE effective_to IS NULL GROUP BY item_id")
    d.strat = _m(cur, "SELECT item_id, MIN(forecast_consumption_strategy::text) FROM item_planning_params WHERE effective_to IS NULL GROUP BY item_id")
    d.names = _m(cur, "SELECT item_id, external_id FROM items")
    d.std_cost = _m(cur, "SELECT item_id, standard_cost FROM items WHERE standard_cost IS NOT NULL")
    d.std_ccy = _m(cur, "SELECT item_id, cost_currency FROM items WHERE standard_cost IS NOT NULL")
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


def consume_demand(d: PlanningData) -> dict:
    """Independent demand per item per bucket AFTER forecast consumption + demand
    time fence. Inside the demand time fence (frozen): customer orders only.
    Beyond: strategy (max_only = max(orders, forecast); never the sum).
    Returns {item: {bucket: qty}}.
    """
    gross = defaultdict(lambda: defaultdict(float))
    for item in set(d.co_b) | set(d.fc_b):
        dtf_weeks = math.ceil(float(d.frozen_d.get(item, 0) or 0) / 7)
        s = (d.strat.get(item) or "max_only").lower()
        for t in set(d.co_b.get(item, {})) | set(d.fc_b.get(item, {})):
            o = d.co_b.get(item, {}).get(t, 0.0)
            f = d.fc_b.get(item, {}).get(t, 0.0)
            if t < dtf_weeks:
                v = o
            elif s == "forecast_only":
                v = f
            elif s == "orders_only":
                v = o
            else:
                v = max(o, f)
            if v:
                gross[item][t] = v
    return gross


def run_timephased(d: PlanningData, gross: dict, force_rule=None, poq_periods=4, trace=None):
    """Time-phased level-by-level MRP cascade. Returns dict with planned orders
    (item, qty, release_bucket, need_bucket, kind, past_due) and counters.

    If `trace` is a list, append per-order explainability tuples
    (item, shortfall_before_lot, qty_after_lot, moq, mult, rule, kind) — used by
    lot-sizing diagnostics.
    """
    dependent = defaultdict(lambda: defaultdict(float))
    planned, rule_orders = [], defaultdict(int)
    n_wo = n_po = past_due = within_ptf = 0
    for level in range(0, d.max_llc + 1):
        for item in d.by_level[level]:
            g = gross.get(item)
            dep = dependent.get(item)
            if not g and not dep:
                continue
            make = bool(d.is_make.get(item, False))
            ss = float(d.safety.get(item, 0) or 0)
            lt_weeks = d.lt_weeks(item)
            im_moq = max(float(d.moq.get(item) or 0), float(d.min_oq.get(item) or 0))
            im_mult = float(d.mult.get(item) or 0)
            ptf_weeks = math.ceil(float(d.slushy_d.get(item, 0) or 0) / 7)
            rule = (force_rule or d.lot_rule.get(item) or "LOTFORLOT").upper()
            P = int((poq_periods if force_rule else d.poq_per.get(item)) or 4)
            eoq = float(d.eoq.get(item) or 0)
            maxoq = float(d.max_oq.get(item) or 0)
            sc = d.sched_b.get(item, {})
            netreq = {}
            for t in range(0, d.n_buckets):
                r = (g.get(t, 0.0) if g else 0.0) + (dep.get(t, 0.0) if dep else 0.0) - sc.get(t, 0.0)
                if r:
                    netreq[t] = r
            pa = float(d.on_hand.get(item, 0) or 0)
            for t in range(0, d.n_buckets):
                pa = pa + sc.get(t, 0.0) - (g.get(t, 0.0) if g else 0.0) - (dep.get(t, 0.0) if dep else 0.0)
                if pa < ss:
                    shortfall = ss - pa
                    qty = apply_lot_rule(rule, shortfall, pa, ss, netreq, t, P, eoq, maxoq, im_moq, im_mult, d.n_buckets)
                    if trace is not None:
                        trace.append((item, shortfall, qty, im_moq, im_mult, rule, "WO" if make else "PO"))
                    pa += qty
                    rel = t - lt_weeks
                    pd = rel < 0
                    if pd:
                        rel = 0
                        past_due += 1
                    if rel < ptf_weeks:
                        within_ptf += 1
                    planned.append((item, qty, rel, t, "WO" if make else "PO", pd))
                    rule_orders[rule] += 1
                    if make:
                        n_wo += 1
                        for comp, qpb, scrap in d.bom.get(item, []):
                            dependent[comp][rel] += qty * qpb * (1.0 + scrap)
                    else:
                        n_po += 1
    return {"planned": planned, "n_wo": n_wo, "n_po": n_po, "past_due": past_due,
            "within_ptf": within_ptf, "rule_orders": dict(rule_orders),
            "dependent": {it: dict(bk) for it, bk in dependent.items()}}


def first_shortage(d: PlanningData, gross: dict) -> dict:
    """Item-level virtual projection on CONSUMED demand (the single demand truth:
    max_only + demand-time-fence + prorated + multi-location-deduped). For each
    item with independent demand, walks weekly buckets accumulating
    scheduled receipts − consumed demand on top of on-hand, and returns the FIRST
    bucket where projected on-hand drops BELOW SAFETY STOCK.

    Triggering at the safety threshold (not at zero/stockout) is deliberate and
    matches run_timephased: safety stock is the reorder trigger that leaves lead
    time to recover, so detecting only at true stockout would fire too late. For
    items with no safety stock (ss=0) this reduces to "first negative bucket".

    `deficit` is the quantity needed to climb back to the safety level (ss − pa),
    i.e. the order is already sized to restore safety — consumers must NOT add ss
    again.

    Returns {item: {"bucket": t, "date": date, "deficit": qty, "balance": pa}}.

    Scope: items carrying independent demand (gross) — the finished-good /
    independent-demand control tower. Dependent-demand (component) shortages are
    the material side (run_timephased), not this projection.
    """
    out = {}
    for item, g in gross.items():
        if not g:
            continue
        ss = float(d.safety.get(item, 0) or 0)
        pa = float(d.on_hand.get(item, 0) or 0)
        sc = d.sched_b.get(item, {})
        for t in range(d.n_buckets):
            pa += sc.get(t, 0.0) - g.get(t, 0.0)
            if pa < ss:
                out[item] = {"bucket": t, "date": d.horizon_start + _dt.timedelta(weeks=t),
                             "deficit": ss - pa, "balance": pa}
                break
    return out


def excess_obsolete(d: PlanningData, gross: dict, months: float = 12.0) -> dict:
    """Classify on-hand stock against its consumption rate and quantify the part
    sitting beyond `months` of coverage.

    Annual usage per item = total GROSS demand over the next 52 weeks (independent
    consumed demand + BOM-exploded dependent demand, no netting — the true burn
    rate). coverage_months = on_hand / (annual / 12).
      EXCESS   : coverage > months → excess_units = on_hand − months × monthly
      OBSOLETE : annual == 0 (no demand on the horizon) → excess_units = on_hand

    Returns {item: {"class","on_hand","annual","coverage_months"(None=∞),"excess_units"}}.
    Only items with on_hand > 0 AND beyond the threshold are returned.
    """
    # Annualize over the available window: sum the first up-to-52 weeks of demand
    # and scale to a full year. With the default horizon (>52 weeks) the factor is
    # 1.0; for a shorter --horizon-days it prevents understating annual demand
    # (which would over-state coverage and mis-flag healthy stock as E&O).
    weeks_win = min(52, d.n_buckets)
    scale = (52.0 / weeks_win) if weeks_win else 1.0
    indep12 = {}
    for item, buckets in gross.items():
        s = sum(q for t, q in buckets.items() if t < weeks_win) * scale
        if s:
            indep12[item] = s
    dep12 = defaultdict(float)
    for level in range(0, d.max_llc + 1):
        for item in d.by_level[level]:
            use = indep12.get(item, 0.0) + dep12.get(item, 0.0)
            if use <= 0 or not bool(d.is_make.get(item, False)):
                continue
            for comp, qpb, scrap in d.bom.get(item, []):
                dep12[comp] += use * qpb * (1.0 + scrap)

    out = {}
    for item, oh in d.on_hand.items():
        oh = float(oh or 0)
        if oh <= 0:
            continue
        annual = indep12.get(item, 0.0) + dep12.get(item, 0.0)
        if annual <= 0:
            out[item] = {"class": "OBSOLETE", "on_hand": oh, "annual": 0.0,
                         "coverage_months": None, "excess_units": oh}
        else:
            monthly = annual / 12.0
            cover = oh / monthly
            if cover <= months:
                continue
            out[item] = {"class": "EXCESS", "on_hand": oh, "annual": annual,
                         "coverage_months": cover, "excess_units": oh - months * monthly}
    return out


def peg_origins(d: PlanningData, gross: dict):
    """Aggregate LLC cascade with origin attribution. Returns (dependent_total,
    origin) where origin[item] = {finished_good: qty}. Uses consumed demand.
    """
    indep_agg = {i: sum(v.values()) for i, v in gross.items()}
    dependent = defaultdict(float)
    origin = defaultdict(lambda: defaultdict(float))
    for level in range(0, d.max_llc + 1):
        for item in d.by_level[level]:
            ind = float(indep_agg.get(item, 0) or 0)
            g_ag = ind + dependent.get(item, 0.0)
            if g_ag <= 0:
                continue
            avail = float(d.on_hand.get(item, 0) or 0) + float(d.firm.get(item, 0) or 0)
            net = g_ag + float(d.safety.get(item, 0) or 0) - avail
            if net <= 0:
                continue
            mix = dict(origin.get(item, {}))
            if ind > 0:
                mix[item] = mix.get(item, 0.0) + ind
            tot = sum(mix.values()) or 1.0
            if bool(d.is_make.get(item, False)):
                for comp, qpb, scrap in d.bom.get(item, []):
                    contrib = net * qpb * (1.0 + scrap)
                    dependent[comp] += contrib
                    oc = origin[comp]
                    for fg, w in mix.items():
                        oc[fg] += contrib * (w / tot)
    return dependent, origin
