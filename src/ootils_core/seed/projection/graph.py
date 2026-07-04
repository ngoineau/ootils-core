"""
graph.py — seed the projection graph (series + PI nodes + edges).

In production, the projection graph is built reactively by the engine
when events come in. For a static benchmark dataset we materialise it
up front for one scope: every (FG, DC) pair gets a 90-day daily PI chain
plus all the edges that drive propagation:

  feeds_forward  PI[t] -> PI[t+1]                       within each series
  replenishes    OnHandSupply(FG, DC) -> PI[0]          opening stock
  replenishes    TransferSupply(FG, DC, ETA) -> PI[i]   in-flight inventory
  consumes       CustomerOrderDemand -> PI[i]           point-in-time
  consumes       ForecastDemand (monthly) -> PI[i..i+30] multi-day span

The 90-day horizon is intentional — wider horizons explode the PI count
(900 K at 365 days) and aren't needed to calibrate shortages or stress
the propagator. Extend `horizon_days` later if downstream scenarios need it.

Volumes expected on profile M:
  projection_series   1 500  (active FGs ~ 477 x 3 DCs ~ 1 431)
  PI nodes          128 790  (1 431 x 90 days)
  feeds_forward     127 359  (1 431 x 89)
  replenishes OH      1 431
  replenishes T-in     ~500
  consumes orders     ~6 000
  consumes forecasts ~12 900  (~3 monthly forecasts per series x ~30 daily PIs)
  ------------------------
  edges total        ~148 K
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date, timedelta
from uuid import UUID, uuid4

from ootils_core.db.types import DictRowConnection
from ootils_core.seed.master.items import ItemSet
from ootils_core.seed.master.locations import LocationSet
from ootils_core.seed.transactional.nodes import BASELINE_SCENARIO_ID


@dataclass
class SeededGraph:
    """Counts produced — fed into the validation report."""
    series_count: int
    pi_node_count: int
    feeds_forward_count: int
    replenishes_from_oh_count: int
    replenishes_from_transfer_count: int
    consumes_from_orders_count: int
    consumes_from_forecasts_count: int
    horizon_start: date
    horizon_end: date  # exclusive
    seconds: float

    @property
    def edges_total(self) -> int:
        return (
            self.feeds_forward_count
            + self.replenishes_from_oh_count
            + self.replenishes_from_transfer_count
            + self.consumes_from_orders_count
            + self.consumes_from_forecasts_count
        )


def seed_projection_graph(
    conn: DictRowConnection,
    item_set: ItemSet,
    loc_set: LocationSet,
    horizon_days: int = 90,
) -> SeededGraph:
    """Materialise the FG-at-DC projection graph for the next `horizon_days` days."""
    started = time.perf_counter()
    today = date.today()
    horizon_start = today
    horizon_end = today + timedelta(days=horizon_days)

    active_fgs = [it for it in item_set.at_level(0) if it.status == "active"]
    dcs = loc_set.dcs()

    # ------------------------------------------------------------------
    # 1. projection_series — one per (FG, DC)
    # ------------------------------------------------------------------
    series_rows: list[tuple[UUID, UUID, UUID, date, date]] = []
    # Map for fast lookup when building PIs and edges
    series_id_by_pair: dict[tuple[UUID, UUID], UUID] = {}
    for fg in active_fgs:
        for dc in dcs:
            sid = uuid4()
            series_rows.append((sid, fg.item_id, dc.location_id, horizon_start, horizon_end))
            series_id_by_pair[(fg.item_id, dc.location_id)] = sid

    conn.execute(
        """
        INSERT INTO projection_series
            (series_id, item_id, location_id, scenario_id, horizon_start, horizon_end)
        SELECT
            s.id, s.item_id, s.loc_id, %s, s.hs, s.he
        FROM UNNEST(%s::uuid[], %s::uuid[], %s::uuid[], %s::date[], %s::date[])
             AS s(id, item_id, loc_id, hs, he)
        """,
        (
            BASELINE_SCENARIO_ID,
            [r[0] for r in series_rows],
            [r[1] for r in series_rows],
            [r[2] for r in series_rows],
            [r[3] for r in series_rows],
            [r[4] for r in series_rows],
        ),
    )

    # ------------------------------------------------------------------
    # 2. PI nodes — 90 daily buckets per series
    # ------------------------------------------------------------------
    pi_ids: list[UUID] = []
    pi_item_id: list[UUID] = []
    pi_loc_id: list[UUID] = []
    pi_bs: list[date] = []
    pi_be: list[date] = []
    pi_series_id: list[UUID] = []
    pi_seq: list[int] = []
    # Track PI ids per (series_id, bucket_sequence) for edge wiring below
    pi_by_series_seq: dict[tuple[UUID, int], UUID] = {}
    for series_id, item_id, loc_id, hs, _he in series_rows:
        for b in range(horizon_days):
            pid = uuid4()
            pi_ids.append(pid)
            pi_item_id.append(item_id)
            pi_loc_id.append(loc_id)
            pi_bs.append(hs + timedelta(days=b))
            pi_be.append(hs + timedelta(days=b + 1))
            pi_series_id.append(series_id)
            pi_seq.append(b)
            pi_by_series_seq[(series_id, b)] = pid

    conn.execute(
        """
        INSERT INTO nodes
            (node_id, node_type, scenario_id, item_id, location_id,
             time_grain, time_span_start, time_span_end,
             projection_series_id, bucket_sequence, is_dirty, active)
        SELECT
            p.id, 'ProjectedInventory', %s, p.item_id, p.loc_id,
            'day', p.bs, p.be, p.sid, p.seq, TRUE, TRUE
        FROM UNNEST(
            %s::uuid[], %s::uuid[], %s::uuid[], %s::date[], %s::date[],
            %s::uuid[], %s::int[]
        ) AS p(id, item_id, loc_id, bs, be, sid, seq)
        """,
        (
            BASELINE_SCENARIO_ID,
            pi_ids, pi_item_id, pi_loc_id, pi_bs, pi_be, pi_series_id, pi_seq,
        ),
    )

    # ------------------------------------------------------------------
    # 3. feeds_forward edges — within each series, PI[t] -> PI[t+1]
    # ------------------------------------------------------------------
    ff_ids: list[UUID] = []
    ff_from: list[UUID] = []
    ff_to: list[UUID] = []
    for series_id, _, _, _, _ in series_rows:
        for b in range(horizon_days - 1):
            ff_ids.append(uuid4())
            ff_from.append(pi_by_series_seq[(series_id, b)])
            ff_to.append(pi_by_series_seq[(series_id, b + 1)])

    if ff_ids:
        conn.execute(
            """
            INSERT INTO edges
                (edge_id, edge_type, from_node_id, to_node_id, scenario_id, active)
            SELECT
                e.id, 'feeds_forward', e.frm, e.dest, %s, TRUE
            FROM UNNEST(%s::uuid[], %s::uuid[], %s::uuid[]) AS e(id, frm, dest)
            """,
            (BASELINE_SCENARIO_ID, ff_ids, ff_from, ff_to),
        )

    # ------------------------------------------------------------------
    # 4. replenishes: OnHandSupply (FG, DC) -> PI[0]
    # ------------------------------------------------------------------
    oh_rows = conn.execute(
        """
        SELECT n.node_id, n.item_id, n.location_id
        FROM nodes n
        JOIN locations l ON l.location_id = n.location_id
        JOIN items i ON i.item_id = n.item_id
        WHERE n.node_type = 'OnHandSupply'
          AND n.active = TRUE
          AND n.scenario_id = %s
          AND l.location_type = 'dc'
          AND i.item_type = 'finished_good'
        """,
        (BASELINE_SCENARIO_ID,),
    ).fetchall()
    rep_oh_ids: list[UUID] = []
    rep_oh_from: list[UUID] = []
    rep_oh_to: list[UUID] = []
    for r in oh_rows:
        pair = (UUID(str(r["item_id"])), UUID(str(r["location_id"])))
        sid_opt: UUID | None = series_id_by_pair.get(pair)
        if sid_opt is None:
            continue
        sid = sid_opt
        pi0 = pi_by_series_seq.get((sid, 0))
        if pi0 is None:
            continue
        rep_oh_ids.append(uuid4())
        rep_oh_from.append(UUID(str(r["node_id"])))
        rep_oh_to.append(pi0)
    if rep_oh_ids:
        conn.execute(
            """
            INSERT INTO edges (edge_id, edge_type, from_node_id, to_node_id, scenario_id, active)
            SELECT e.id, 'replenishes', e.frm, e.dest, %s, TRUE
            FROM UNNEST(%s::uuid[], %s::uuid[], %s::uuid[]) AS e(id, frm, dest)
            """,
            (BASELINE_SCENARIO_ID, rep_oh_ids, rep_oh_from, rep_oh_to),
        )

    # ------------------------------------------------------------------
    # 5. replenishes: TransferSupply (FG, DC) with ETA in window -> PI[i]
    # ------------------------------------------------------------------
    t_rows = conn.execute(
        """
        SELECT n.node_id, n.item_id, n.location_id, n.time_ref
        FROM nodes n
        JOIN locations l ON l.location_id = n.location_id
        WHERE n.node_type = 'TransferSupply'
          AND n.active = TRUE
          AND n.scenario_id = %s
          AND l.location_type = 'dc'
          AND n.time_ref >= %s AND n.time_ref < %s
        """,
        (BASELINE_SCENARIO_ID, horizon_start, horizon_end),
    ).fetchall()
    rep_t_ids: list[UUID] = []
    rep_t_from: list[UUID] = []
    rep_t_to: list[UUID] = []
    for r in t_rows:
        pair = (UUID(str(r["item_id"])), UUID(str(r["location_id"])))
        sid_opt = series_id_by_pair.get(pair)
        if sid_opt is None:
            continue
        sid = sid_opt
        eta: date = r["time_ref"]
        seq = (eta - horizon_start).days
        pi = pi_by_series_seq.get((sid, seq))
        if pi is None:
            continue
        rep_t_ids.append(uuid4())
        rep_t_from.append(UUID(str(r["node_id"])))
        rep_t_to.append(pi)
    if rep_t_ids:
        conn.execute(
            """
            INSERT INTO edges (edge_id, edge_type, from_node_id, to_node_id, scenario_id, active)
            SELECT e.id, 'replenishes', e.frm, e.dest, %s, TRUE
            FROM UNNEST(%s::uuid[], %s::uuid[], %s::uuid[]) AS e(id, frm, dest)
            """,
            (BASELINE_SCENARIO_ID, rep_t_ids, rep_t_from, rep_t_to),
        )

    # ------------------------------------------------------------------
    # 6. consumes: open CustomerOrderDemand -> PI[bucket containing time_ref]
    # ------------------------------------------------------------------
    co_rows = conn.execute(
        """
        SELECT node_id, item_id, location_id, time_ref
        FROM nodes
        WHERE node_type = 'CustomerOrderDemand'
          AND active = TRUE
          AND scenario_id = %s
          AND time_ref >= %s AND time_ref < %s
        """,
        (BASELINE_SCENARIO_ID, horizon_start, horizon_end),
    ).fetchall()
    co_ids: list[UUID] = []
    co_from: list[UUID] = []
    co_to: list[UUID] = []
    for r in co_rows:
        pair = (UUID(str(r["item_id"])), UUID(str(r["location_id"])))
        sid_opt = series_id_by_pair.get(pair)
        if sid_opt is None:
            continue
        sid = sid_opt
        seq = (r["time_ref"] - horizon_start).days
        pi = pi_by_series_seq.get((sid, seq))
        if pi is None:
            continue
        co_ids.append(uuid4())
        co_from.append(UUID(str(r["node_id"])))
        co_to.append(pi)
    if co_ids:
        conn.execute(
            """
            INSERT INTO edges (edge_id, edge_type, from_node_id, to_node_id, scenario_id, active)
            SELECT e.id, 'consumes', e.frm, e.dest, %s, TRUE
            FROM UNNEST(%s::uuid[], %s::uuid[], %s::uuid[]) AS e(id, frm, dest)
            """,
            (BASELINE_SCENARIO_ID, co_ids, co_from, co_to),
        )

    # ------------------------------------------------------------------
    # 7. consumes: ForecastDemand (monthly span) -> PI[i] for each overlapping bucket
    # ------------------------------------------------------------------
    fc_rows = conn.execute(
        """
        SELECT node_id, item_id, location_id, time_span_start, time_span_end
        FROM nodes
        WHERE node_type = 'ForecastDemand'
          AND active = TRUE
          AND scenario_id = %s
          AND time_span_start < %s
          AND time_span_end   > %s
        """,
        (BASELINE_SCENARIO_ID, horizon_end, horizon_start),
    ).fetchall()
    fc_ids: list[UUID] = []
    fc_from: list[UUID] = []
    fc_to: list[UUID] = []
    for r in fc_rows:
        pair = (UUID(str(r["item_id"])), UUID(str(r["location_id"])))
        sid_opt = series_id_by_pair.get(pair)
        if sid_opt is None:
            continue
        sid = sid_opt
        # Iterate days the forecast overlaps the horizon
        ovl_start = max(horizon_start, r["time_span_start"])
        ovl_end = min(horizon_end, r["time_span_end"])
        seq = (ovl_start - horizon_start).days
        days = (ovl_end - ovl_start).days
        forecast_node_id = UUID(str(r["node_id"]))
        for k in range(days):
            pi = pi_by_series_seq.get((sid, seq + k))
            if pi is None:
                continue
            fc_ids.append(uuid4())
            fc_from.append(forecast_node_id)
            fc_to.append(pi)
    if fc_ids:
        conn.execute(
            """
            INSERT INTO edges (edge_id, edge_type, from_node_id, to_node_id, scenario_id, active)
            SELECT e.id, 'consumes', e.frm, e.dest, %s, TRUE
            FROM UNNEST(%s::uuid[], %s::uuid[], %s::uuid[]) AS e(id, frm, dest)
            """,
            (BASELINE_SCENARIO_ID, fc_ids, fc_from, fc_to),
        )

    conn.commit()

    # ------------------------------------------------------------------
    # Update query planner stats — without this the first propagation run
    # uses stale row-count estimates and picks seq scans where indexes would
    # be 50x faster. Observed first-iter propagation time on profile S
    # dropped from 535 s to ~10 s after this.
    # ------------------------------------------------------------------
    conn.execute("ANALYZE nodes")
    conn.execute("ANALYZE edges")
    conn.execute("ANALYZE projection_series")
    conn.commit()

    elapsed = time.perf_counter() - started

    return SeededGraph(
        series_count=len(series_rows),
        pi_node_count=len(pi_ids),
        feeds_forward_count=len(ff_ids),
        replenishes_from_oh_count=len(rep_oh_ids),
        replenishes_from_transfer_count=len(rep_t_ids),
        consumes_from_orders_count=len(co_ids),
        consumes_from_forecasts_count=len(fc_ids),
        horizon_start=horizon_start,
        horizon_end=horizon_end,
        seconds=round(elapsed, 2),
    )
