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

import psycopg

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
    return lot_size(qty, moq, mult)


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

    for parent, comp, qpb, scrap in cur.execute(
        "SELECT bh.parent_item_id, bl.component_item_id, bl.quantity_per, bl.scrap_factor "
        "FROM bom_headers bh JOIN bom_lines bl ON bl.bom_id=bh.bom_id "
        "WHERE bh.effective_to IS NULL OR bh.effective_to > CURRENT_DATE").fetchall():
        d.bom.setdefault(parent, []).append((comp, float(qpb), float(scrap or 0)))

    for item, sid, sext, lt, uc, ccy, rel in cur.execute(
        "SELECT DISTINCT ON (si.item_id) si.item_id, s.supplier_id, s.external_id, "
        "si.lead_time_days, si.unit_cost, si.currency, s.reliability_score "
        "FROM supplier_items si JOIN suppliers s ON s.supplier_id=si.supplier_id "
        "WHERE si.lead_time_days IS NOT NULL "
        "ORDER BY si.item_id, si.is_preferred DESC, si.lead_time_days ASC").fetchall():
        d.best_sup[item] = (sid, sext, lt, uc, ccy, rel)

    d.co_b = defaultdict(lambda: defaultdict(float))
    for item, tref, qty in cur.execute(
        "SELECT item_id, time_ref, quantity FROM nodes WHERE scenario_id=%(b)s AND active "
        "AND node_type='CustomerOrderDemand' AND time_ref IS NOT NULL AND quantity IS NOT NULL", b).fetchall():
        if tref >= horizon_start:
            d.co_b[item][d.bucket(tref)] += float(qty)
    d.fc_b = defaultdict(lambda: defaultdict(float))
    for item, tref, qty in cur.execute(
        "SELECT item_id, time_ref, quantity FROM nodes WHERE scenario_id=%(b)s AND active "
        "AND node_type='ForecastDemand' AND time_ref IS NOT NULL AND quantity IS NOT NULL", b).fetchall():
        if tref >= horizon_start:
            d.fc_b[item][d.bucket(tref)] += float(qty)
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


def run_timephased(d: PlanningData, gross: dict, force_rule=None, poq_periods=4):
    """Time-phased level-by-level MRP cascade. Returns dict with planned orders
    (item, qty, release_bucket, need_bucket, kind, past_due) and counters.
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
                    qty = apply_lot_rule(rule, ss - pa, pa, ss, netreq, t, P, eoq, maxoq, im_moq, im_mult, d.n_buckets)
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
            "within_ptf": within_ptf, "rule_orders": dict(rule_orders)}


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
