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

# Reschedule dampening defaults — mirror the item_planning_params column
# DEFAULTs (migration 061): don't emit a message for a sub-material date/qty
# nudge. Used when an item carries no explicit per-item threshold.
DEFAULT_RESCHEDULE_MIN_DAYS = 3
DEFAULT_RESCHEDULE_QTY_TOLERANCE_PCT = 5.0

# Reschedule actions the core emits (#346 PR-A). DEFER is in the migration-061
# CHECK vocabulary but is DELIBERATELY not produced here: it is reserved for
# manual/agent use (a planner choosing to push an order without a computed
# need-date). The deterministic core only emits the three data-driven actions.
RESCHEDULE_IN = "RESCHEDULE_IN"
RESCHEDULE_OUT = "RESCHEDULE_OUT"
CANCEL = "CANCEL"


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


@dataclass(frozen=True)
class ReceiptOrder:
    """A single closed/firm receipt with its identity preserved (#346).

    Unlike PlanningData.sched_b (which pre-aggregates receipts into weekly
    buckets and so LOSES the per-order identity), a ReceiptOrder keeps the
    (node_id, receipt_date, qty) triple so the reschedule pass can propose a
    new date for THIS specific order. is_firm distinguishes a firmed planned
    order (FPO) from an ordinary closed receipt; node_type is retained for
    downstream message attribution.
    """

    node_id: str
    item_id: str
    receipt_date: _dt.date
    qty: float
    is_firm: bool
    node_type: str


@dataclass(frozen=True)
class RescheduleSignal:
    """A dampened reschedule action message for one receipt (#346 PR-A).

    action is one of RESCHEDULE_IN / RESCHEDULE_OUT / CANCEL. proposed_date is
    None for CANCEL (no new date — the whole receipt is surplus). qty is the
    receipt's own quantity; the message targets the receipt as a unit (V1 does
    not split a receipt).

    node_type / is_firm are carried through verbatim from the source
    ReceiptOrder so PR-B (governed emission + decision_level) can attribute the
    message to the right supply kind without re-fetching the node.
    """

    node_id: str
    item_id: str
    action: str
    current_receipt_date: _dt.date
    proposed_date: _dt.date | None
    qty: float
    node_type: str
    is_firm: bool


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
    # Per-item list of closed/firm receipts WITH identity (#346). Parallel to
    # sched_b (the bucket aggregate kept for projection); reschedule uses this.
    sched_orders: dict = field(default_factory=dict)
    # Dampening thresholds (baseline-only, migration 061). Missing key => the
    # module DEFAULT_* is used by reschedule_signals.
    resched_min_days: dict = field(default_factory=dict)
    resched_qty_tol_pct: dict = field(default_factory=dict)
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


def _need_bucket_for_receipts(
    orders: list[ReceiptOrder], on_hand: float, ss: float, g: dict, n_buckets: int
) -> dict[str, int | None]:
    """Derive a NEED BUCKET for each firm receipt of one item (#346, PURE).

    Convention (single source of the need-date semantics): walk weekly buckets
    forward on a running projected-on-hand balance. Start at on_hand. Each
    bucket subtracts that bucket's consumed independent demand (g). Safety stock
    is a floor: the balance must stay at or above ss, so a requirement appears as
    soon as the balance (before applying receipts) would fall below ss.

    Receipts are matched to requirements FIFO by their OWN date (earliest receipt
    covers the earliest requirement). The need bucket of a receipt is the bucket
    where the CENTRE OF GRAVITY of its own consumption sits — precisely the
    bucket at which the cumulative quantity allocated to that receipt first
    reaches 50% of the receipt quantity (its median / median-unit bucket). This
    is deliberately NOT the first requirement the receipt marginally touches: a
    receipt of 200 that covers 10 @wk1 + 190 @wk10 has its median unit consumed
    at wk10, so its need bucket is wk10 — proposing to pull all 200 units to wk1
    (56 days early for 190 of them) would be over-eager. The median rule dates
    the order by where the bulk of it is actually pulled.

    A receipt whose quantity is never pulled at all (all demand already covered
    by on-hand + earlier receipts) has need bucket None => CANCEL candidate.

    Returns {node_id: need_bucket or None}. Deterministic: receipts are sorted by
    (receipt_date, node_id) so ties never depend on input ordering, and the 50%
    crossing uses a fixed tolerance.
    """
    ordered = sorted(orders, key=lambda o: (o.receipt_date, o.node_id))
    # Cumulative demand requirement (net of on-hand + safety) per bucket. balance
    # walks down; each bucket's shortfall below ss is a requirement quantity that
    # must be met at that bucket.
    requirements: list[tuple[int, float]] = []
    balance = on_hand
    for t in range(n_buckets):
        balance -= g.get(t, 0.0)
        if balance < ss:
            requirements.append((t, ss - balance))
            balance = ss  # requirement is deemed met; keep walking for later ones
    # FIFO allocation: consume requirement quantity with receipts in date order.
    # For each receipt track its allocated-so-far running total; its need bucket
    # is the requirement bucket at which that total first reaches half its qty.
    need: dict[str, int | None] = {o.node_id: None for o in ordered}
    ri = 0
    remaining = requirements[ri][1] if requirements else 0.0
    for o in ordered:
        supply = o.qty
        allocated = 0.0
        half = o.qty / 2.0
        while supply > 0 and ri < len(requirements):
            req_bucket = requirements[ri][0]
            take = min(supply, remaining)
            if take > 0:
                allocated += take
                # Median-unit rule: the need bucket is where cumulative
                # allocation first covers 50% of the receipt (centre of gravity).
                if need[o.node_id] is None and allocated >= half - 1e-9:
                    need[o.node_id] = req_bucket
            supply -= take
            remaining -= take
            if remaining <= 1e-9:
                ri += 1
                remaining = requirements[ri][1] if ri < len(requirements) else 0.0
    return need


def reschedule_signals(d: PlanningData, gross: dict) -> list[RescheduleSignal]:
    """Canonical receipt-vs-need comparison (#346 PR-A). PURE, DB-free.

    For every item with firm receipts (d.sched_orders), derive each receipt's
    NEED DATE from the time-phased projection (see _need_bucket_for_receipts),
    compare it to the receipt's current date, and emit a DAMPENED action message:

      RESCHEDULE_IN  — receipt arrives AFTER its need date (pull it earlier).
      RESCHEDULE_OUT — receipt arrives BEFORE its need date (push it later to
                       free stock/cash).
      CANCEL         — receipt has no matching need on the horizon (surplus).

    Dampening (mandatory, anti message-storm):
      * a RESCHEDULE is emitted only if |proposed - current| >= reschedule_min_days.
      * a CANCEL is emitted for any firm receipt that is ENTIRELY surplus
        (need bucket None) — with a horizon-edge guard (see below). V1 does NOT
        apply reschedule_qty_tolerance_pct to CANCEL: a receipt that reaches the
        CANCEL branch is 100% surplus by construction, so a graded quantity
        tolerance is meaningless without splitting the receipt. The threshold is
        RESERVED for the V2 partial-split path (emit a CANCEL only for the
        surplus fraction of a partially-needed receipt); it is intentionally a
        no-op in V1. Not pretending a graded semantics that does not exist.

    Horizon-edge CANCEL guard (anti false-positive): a receipt landing in the
    LAST bucket of the loaded horizon is NEVER cancelled even when it has no
    matching need. The demand that justifies it may simply sit just beyond the
    loaded window (visibility, not surplus): cancelling on incomplete horizon
    visibility is unsafe. One bucket (~1 week) of margin is the V1 threshold —
    the minimum that removes the boundary artefact without masking a genuine
    surplus that sits well inside the horizon.

    THE CENTRAL INVARIANT: re-running on unchanged data emits ZERO signals — a
    receipt already on (or within dampening of) its need date produces no message.
    Stability is the goal.

    Deterministic: items and, within an item, receipts are processed in a stable
    sorted order; the returned list is sorted by (item_id, node_id).

    DEFER is not emitted (reserved for manual/agent use — see module constants).
    """
    signals: list[RescheduleSignal] = []
    # Receipts at or past this bucket are on the horizon edge: no CANCEL there
    # (their justifying demand may fall just beyond the loaded window).
    cancel_cutoff_bucket = d.n_buckets - 1
    for item in sorted(d.sched_orders):
        orders = d.sched_orders.get(item) or []
        if not orders:
            continue
        on_hand = float(d.on_hand.get(item, 0) or 0)
        ss = float(d.safety.get(item, 0) or 0)
        g = gross.get(item) or {}
        md = d.resched_min_days.get(item)
        min_days = int(md) if md is not None else DEFAULT_RESCHEDULE_MIN_DAYS
        need = _need_bucket_for_receipts(orders, on_hand, ss, g, d.n_buckets)
        for o in sorted(orders, key=lambda x: (x.receipt_date, x.node_id)):
            nb = need.get(o.node_id)
            if nb is None:
                # Entirely surplus (no need on the horizon) => CANCEL, UNLESS the
                # receipt sits on the horizon edge, where its demand may be just
                # out of the loaded window (guard against a phantom CANCEL).
                if o.qty > 0 and d.bucket(o.receipt_date) < cancel_cutoff_bucket:
                    signals.append(RescheduleSignal(
                        node_id=o.node_id, item_id=item, action=CANCEL,
                        current_receipt_date=o.receipt_date, proposed_date=None,
                        qty=o.qty, node_type=o.node_type, is_firm=o.is_firm))
                continue
            # The projection resolves need at BUCKET granularity, so a receipt
            # already sitting in its need bucket is on time by construction — no
            # message, whatever its intra-bucket weekday. This is what makes the
            # stability invariant hold (re-run on unchanged data => 0 signals).
            if d.bucket(o.receipt_date) == nb:
                continue
            proposed = d.horizon_start + _dt.timedelta(weeks=nb)
            delta_days = (proposed - o.receipt_date).days
            if abs(delta_days) < min_days:
                continue  # within dampening: no message (the stability path)
            action = RESCHEDULE_IN if delta_days < 0 else RESCHEDULE_OUT
            signals.append(RescheduleSignal(
                node_id=o.node_id, item_id=item, action=action,
                current_receipt_date=o.receipt_date, proposed_date=proposed,
                qty=o.qty, node_type=o.node_type, is_firm=o.is_firm))
    signals.sort(key=lambda s: (s.item_id, s.node_id))
    return signals


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
