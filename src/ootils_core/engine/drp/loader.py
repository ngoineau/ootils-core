"""
DB loading layer for DRP (distribution) planning data (ADR-020 / ADR-028).
SELECT-only; core.py is DB-free. Mirrors engine/mrp/loader.py's style — one
scenario-scoped scan per source, tuple-row cursor, `_spread_period` forecast
proration — with ONE structural difference: everything is keyed by
(item, location) instead of pooled to item level. The distribution echelon
plans per-site; the MRP echelon pools. See ADR-020 §Unité de planification.

Scenario-parameterized like the MRP loader: `scenario` scopes the node reads
(a fork sees its own on-hand / demand nodes) and drives the planning-param
overlay (#347), so a fork's safety-stock override is visible to the DRP
projection — which is what makes the distribution plan forkable (agents can
test a safety-stock counter-factual per site without forking master data).

This module NEVER writes and NEVER commits (SELECT-only; the caller owns the
transaction, same contract as the MRP loader and ScenarioManager).
"""
from __future__ import annotations

import datetime as _dt
import math
from collections import defaultdict
from dataclasses import dataclass, field

from psycopg.rows import tuple_row

from ootils_core.engine.drp.core import TransferLink
from ootils_core.engine.mrp.core import BASELINE, _spread_period
from ootils_core.engine.scenario.param_overlay import resolved_params_sql

# Independent-demand node types the DRP echelon nets, mirroring
# mrp/core.DEMAND_TYPES. Kept as a local constant (not imported) so a future
# change to the MRP demand vocabulary is a conscious cross-echelon decision.
DRP_DEMAND_TYPES = ["CustomerOrderDemand", "ForecastDemand"]

# Planning-key convention (#395 F4): items.external_id and locations.external_id
# are BOTH nullable (migration 007's backfill was one-shot; the UNIQUE
# constraint tolerates any number of NULLs), and the repo's OWN seed pipeline
# (seed/master/locations.py, seed/master/items.py) inserts rows WITHOUT
# external_id. Keying purely on external_id would therefore collapse every
# never-backfilled item (or location) onto the single string "None" for that
# column, and — because Python's f"{None}" == "None" for every row alike, not a
# per-row unique value — every site with a NULL external_id would silently pool
# onto ONE shared coordinate. That is exactly the per-site separation this
# module exists to preserve, so it must never depend on external_id alone.
#
# The fix is a deterministic fallback: COALESCE(external_id, <uuid>::text).
# When external_id is set, the coordinate carries the readable business key
# (unchanged from before — no behavioural difference on a fully-backfilled
# dataset). When it is NULL, the coordinate falls back to the row's OWN UUID
# stringified — unique and stable per row (the same item/location always
# yields the same fallback key across calls, since UUIDs don't change), so
# per-site separation holds even on an un-backfilled dataset. This convention
# is applied to EVERY query in this loader (on-hand, customer orders, forecast,
# safety stock, distribution links) — one rule, no exceptions, so a coordinate
# computed from one query always matches the same coordinate from another.
#
# Parameterized by table alias (not hardcoded to `i`/`l`) because the
# distribution-links query joins TWO location aliases (su/du, upstream/
# downstream) and needs the SAME convention applied to each independently —
# a plain module-level string constant could only ever name one alias, so a
# tiny function is the correct shape here, not a fixed literal.
def _item_key_sql(alias: str) -> str:
    return f"COALESCE({alias}.external_id, {alias}.item_id::text)"


def _loc_key_sql(alias: str) -> str:
    return f"COALESCE({alias}.external_id, {alias}.location_id::text)"


@dataclass
class DRPData:
    """Loaded, scenario-resolved inputs for the DRP core, all keyed by the
    (item, location) planning coordinate.

    Key convention (#395 F4): each coordinate component is
    COALESCE(external_id, <uuid>::text) — the item/location's business
    external_id when it is set, or the row's own UUID stringified when it is
    NULL. NEVER the bare external_id: that column is nullable on BOTH items
    and locations, and this repo's own seed pipeline inserts rows without one,
    so a bare-external_id key would silently pool every un-backfilled site onto
    one shared "None" coordinate — the exact per-site collapse this module
    exists to prevent. See _item_key_sql / _loc_key_sql below for the single
    fragment implementing this, applied identically to every query.

    demand_by_loc : {(item, location): {bucket: net_demand}} — per-bucket
        max(orders, forecast) (see the loader; the #349 windowed consumption is
        item-level and a per-location variant is PR2+).
    on_hand_by_loc: {(item, location): qty} — summed OnHandSupply of the scenario.
    safety_by_loc : {(item, location): qty} — overlay-resolved safety_stock_qty
        (#347), NOT pooled across locations.
    links         : active distribution_links as TransferLink compute records.
    horizon_start / n_buckets carried for callers that need to date a bucket
        back to a calendar date (parallel to PlanningData.horizon_start).
    """

    horizon_start: _dt.date
    n_buckets: int
    demand_by_loc: dict[tuple[str, str], dict[int, float]] = field(default_factory=dict)
    on_hand_by_loc: dict[tuple[str, str], float] = field(default_factory=dict)
    safety_by_loc: dict[tuple[str, str], float] = field(default_factory=dict)
    links: list[TransferLink] = field(default_factory=list)


def load_drp_data(conn, horizon_days: int = 180, scenario: str = BASELINE) -> DRPData:
    # tuple_row for positional access regardless of the connection's configured
    # row_factory (a scenario-aware caller hands us the app's dict_row
    # connection — same reason as the MRP loader).
    cur = conn.cursor(row_factory=tuple_row)
    horizon_start = cur.execute("SELECT CURRENT_DATE").fetchone()[0]
    n_buckets = math.ceil(horizon_days / 7) + 1
    horizon_end = horizon_start + _dt.timedelta(days=horizon_days)

    def bucket(d: _dt.date) -> int:
        return max(0, (d - horizon_start).days // 7)

    d = DRPData(horizon_start=horizon_start, n_buckets=n_buckets)

    # --- on-hand per (item, location) ---------------------------------------
    # OnHandSupply of the scenario, summed per coordinate. A node with a NULL
    # location_id is skipped: an un-located on-hand has no place in a per-site
    # distribution plan (the MRP echelon, which pools, is where an un-located
    # quantity still counts). Scenario-scoped like the MRP loader's nodes scan.
    on_hand: defaultdict[tuple[str, str], float] = defaultdict(float)
    for item, loc, qty in cur.execute(
        f"SELECT {_item_key_sql('i')}, {_loc_key_sql('l')}, n.quantity "
        "FROM nodes n "
        "JOIN items i ON i.item_id = n.item_id "
        "JOIN locations l ON l.location_id = n.location_id "
        "WHERE n.scenario_id = %(b)s AND n.active "
        "AND n.node_type = 'OnHandSupply' "
        "AND n.quantity IS NOT NULL",
        {"b": scenario},
    ).fetchall():
        on_hand[(item, loc)] += float(qty)
    d.on_hand_by_loc = dict(on_hand)

    # --- customer-order demand per (item, location), weekly buckets ----------
    # #395 F1: clipped to [horizon_start, horizon_end] — SYMMETRIC with the
    # forecast side below, which _spread_period already confines to
    # [horizon_start, horizon_end). Before this fix only the lower bound was
    # enforced (tref >= horizon_start) and a customer order dated well beyond
    # the loaded horizon (e.g. horizon_days=180 but tref at +400 days) still
    # landed in bucket((tref)) — a bucket FAR past n_buckets, invisible to
    # excess_by_location's now-windowed sum (see core.py's matching F1 fix) and
    # to projected_deficits' `range(horizon_buckets)` walk, but silently present
    # in co_b/demand_by_loc's raw dict — a discarded-looking key that could
    # still leak into a future consumer iterating demand_by_loc directly. The
    # upper clip keeps demand_by_loc itself confined to the SAME window the
    # core functions actually see, so the loader and the core never disagree
    # about what "the horizon" contains.
    co_b: defaultdict[tuple[str, str], defaultdict[int, float]] = defaultdict(
        lambda: defaultdict(float)
    )
    for item, loc, tref, qty in cur.execute(
        f"SELECT {_item_key_sql('i')}, {_loc_key_sql('l')}, n.time_ref, n.quantity "
        "FROM nodes n "
        "JOIN items i ON i.item_id = n.item_id "
        "JOIN locations l ON l.location_id = n.location_id "
        "WHERE n.scenario_id = %(b)s AND n.active "
        "AND n.node_type = 'CustomerOrderDemand' "
        "AND n.time_ref IS NOT NULL AND n.quantity IS NOT NULL",
        {"b": scenario},
    ).fetchall():
        if horizon_start <= tref <= horizon_end:
            co_b[(item, loc)][bucket(tref)] += float(qty)

    # --- forecast demand per (item, location), prorated to weekly buckets ----
    # Same proration as the MRP loader (each line spread from its date to the
    # NEXT forecast date for the SAME series), but the series key is
    # (item, location) — NOT pooled to item. Aggregate by (item, location, date)
    # first so duplicate dates collapse and the period-to-next-date span is never
    # zero-length (which would silently drop volume — the MRP loader guards the
    # same way, there against multi-location dupes it is intentionally pooling;
    # here the location is part of the key, so we only collapse true same-site
    # duplicate-date lines).
    raw_fc: defaultdict[tuple[str, str], defaultdict[_dt.date, float]] = defaultdict(
        lambda: defaultdict(float)
    )
    for item, loc, tref, qty in cur.execute(
        f"SELECT {_item_key_sql('i')}, {_loc_key_sql('l')}, n.time_ref, n.quantity "
        "FROM nodes n "
        "JOIN items i ON i.item_id = n.item_id "
        "JOIN locations l ON l.location_id = n.location_id "
        "WHERE n.scenario_id = %(b)s AND n.active "
        "AND n.node_type = 'ForecastDemand' "
        "AND n.time_ref IS NOT NULL AND n.quantity IS NOT NULL",
        {"b": scenario},
    ).fetchall():
        raw_fc[(item, loc)][tref] += float(qty)

    fc_b: defaultdict[tuple[str, str], defaultdict[int, float]] = defaultdict(
        lambda: defaultdict(float)
    )
    for coord, datemap in raw_fc.items():
        rows = sorted(datemap.items())
        gaps = [
            (rows[i + 1][0] - rows[i][0]).days
            for i in range(len(rows) - 1)
            if (rows[i + 1][0] - rows[i][0]).days > 0
        ]
        default_span = sorted(gaps)[len(gaps) // 2] if gaps else 7  # median gap, else weekly
        for i, (tref, qty) in enumerate(rows):
            end = rows[i + 1][0] if i + 1 < len(rows) else tref + _dt.timedelta(days=default_span)
            _spread_period(qty, tref, end, horizon_start, horizon_end, n_buckets, fc_b[coord])

    # --- net demand = per-bucket max(orders, forecast) per coordinate --------
    # DRP echelon V1 net-demand rule: for each coordinate and bucket, the net
    # independent demand is max(customer orders, forecast) — exactly what
    # mrp/core.consume_demand reduces to under the default max_only strategy with
    # a ZERO consumption window (the golden-master semantics). The forecast-
    # consumption WINDOW of #349 (a booking consuming a neighbouring bucket's
    # forecast, backward-before-forward) and the demand time fence are per-item
    # machinery in consume_demand that need PlanningData; a per-location variant
    # is a deliberate PR2+ refinement (module docstring of core.py). Summing
    # orders + forecast here instead of maxing would double-count the forecast a
    # booking already consumes.
    demand_by_loc: dict[tuple[str, str], dict[int, float]] = {}
    empty: dict[int, float] = {}
    for coord in set(co_b) | set(fc_b):
        orders: dict[int, float] = co_b.get(coord, empty)
        forecast: dict[int, float] = fc_b.get(coord, empty)
        merged: dict[int, float] = {}
        for t in set(orders) | set(forecast):
            v = max(orders.get(t, 0.0), forecast.get(t, 0.0))
            if v:
                merged[t] = v
        if merged:
            demand_by_loc[coord] = merged
    d.demand_by_loc = demand_by_loc

    # --- safety stock per (item, location), overlay-resolved (#347) ----------
    # resolved_params_sql() yields ONE row per (item_id, location_id) with
    # safety_stock_qty already COALESCEd against any scenario override — so a
    # fork's per-site safety override is visible here WITHOUT pooling (the MRP
    # loader pools SUM(safety) across locations for its item-level key; the DRP
    # echelon keeps the per-location value, which is the whole point). Baseline
    # (scenario == BASELINE) passes scenario_id=None so every LATERAL override
    # join degrades to "no row" and the base column flows through unchanged.
    resolved = resolved_params_sql("ipp")
    overlay_scenario_id = scenario if scenario != BASELINE else None
    safety: dict[tuple[str, str], float] = {}
    for item, loc, ss in cur.execute(
        f"""
        SELECT {_item_key_sql('i')}, {_loc_key_sql('l')}, rp.safety_stock_qty
        FROM ({resolved}) rp
        JOIN items i ON i.item_id = rp.item_id
        JOIN locations l ON l.location_id = rp.location_id
        """,
        {"scenario_id": overlay_scenario_id},
    ).fetchall():
        if ss is not None:
            safety[(item, loc)] = float(ss)
    d.safety_by_loc = safety

    # --- active distribution links -> TransferLink ---------------------------
    # lead_buckets = ceil(transit_lead_time_days / 7). Locations resolved to the
    # same COALESCE(external_id, uuid::text) key convention as everywhere else
    # in this loader (#395 F4 — see _loc_key_sql's docstring above); su/du are
    # the upstream/downstream location aliases, so the same alias-parameterized
    # helper is invoked once per alias rather than the shared `l` used by the
    # other queries. max_qty is None when the column is NULL (uncapped). min_qty
    # defaults to the column DEFAULT (1) via NOT NULL; read straight.
    # distribution_links is NOT scenario-scoped (network topology is master
    # data, shared across scenarios) — a fork overlays parametric fields
    # (safety), not topology, per ADR-025's whitelist.
    #
    # item_id (#395 F2/F3): distribution_links.item_id is NULLABLE — NULL means
    # a GENERIC lane (any item may use it), a set value means the lane is
    # scoped to that one item. LEFT JOIN items (not an inner JOIN) because a
    # generic link's item_id is NULL and must NOT be dropped by the join; the
    # resolved item key is itself NULL when item_id IS NULL (COALESCE only
    # applies once ii.item_id is present — a NULL LEFT JOIN miss propagates
    # through COALESCE(ii.external_id, ii.item_id::text) as NULL, which is
    # exactly TransferLink.item's None-means-generic contract, so no special
    # casing is needed in the loop below beyond checking `is not None`).
    #
    # ORDER BY / link_ref (#395 F6): without a fixed order, two distribution_
    # links rows on the SAME (source, dest) pair at the SAME priority would
    # have their relative order decided by physical scan order — not a
    # business signal, and it would make the emitted plan depend on
    # incidental table/index layout. ORDER BY (priority, source, dest,
    # distribution_link_id) fixes that ordering deterministically at the SQL
    # layer; link_ref (the stringified distribution_link_id) is carried onto
    # TransferLink as the final, last-resort tie-break the core sorts by (see
    # core.py's _resolve_candidate_links / transfer_signals) — belt AND
    # braces: the loader emits a stable order, and the core's own sort keys
    # never depend on it holding.
    links: list[TransferLink] = []
    for src, dst, lt_days, min_q, max_q, prio, item_key, link_id in cur.execute(
        f"SELECT {_loc_key_sql('su')}, {_loc_key_sql('du')}, "
        "dl.transit_lead_time_days, dl.minimum_shipment_qty, "
        "dl.maximum_shipment_qty, dl.priority, "
        "COALESCE(ii.external_id, ii.item_id::text), dl.distribution_link_id "
        "FROM distribution_links dl "
        "JOIN locations su ON su.location_id = dl.upstream_location_id "
        "JOIN locations du ON du.location_id = dl.downstream_location_id "
        "LEFT JOIN items ii ON ii.item_id = dl.item_id "
        "WHERE dl.active "
        "ORDER BY dl.priority, su.location_id, du.location_id, dl.distribution_link_id"
    ).fetchall():
        links.append(TransferLink(
            source_location=src,
            dest_location=dst,
            lead_buckets=math.ceil(float(lt_days) / 7),
            min_qty=float(min_q),
            max_qty=float(max_q) if max_q is not None else None,
            priority=int(prio),
            item=item_key,
            link_ref=str(link_id),
        ))
    d.links = links

    return d
