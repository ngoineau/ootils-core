"""
Canonical packaged MRP math core (ADR-020). DB-free.
Planning key = item-level today; parameterizable to (item, location) for the
per-site DRP echelon (ADR-020 §Unité de planification).
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


def consume_demand(d: PlanningData) -> dict:
    """Independent demand per item per bucket AFTER forecast consumption + demand
    time fence. Inside the demand time fence (frozen): customer orders only.
    Beyond: strategy (max_only = max(orders, forecast); never the sum).
    Returns {item: {bucket: qty}}.
    """
    gross: defaultdict[str, defaultdict[int, float]] = defaultdict(lambda: defaultdict(float))
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
    dependent: defaultdict[str, defaultdict[int, float]] = defaultdict(lambda: defaultdict(float))
    planned: list = []
    rule_orders: defaultdict[str, int] = defaultdict(int)
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
    dep12: defaultdict[str, float] = defaultdict(float)
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
    dependent: defaultdict[str, float] = defaultdict(float)
    origin: defaultdict[str, defaultdict[str, float]] = defaultdict(lambda: defaultdict(float))
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
