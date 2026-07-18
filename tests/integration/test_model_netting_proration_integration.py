"""
tests/integration/test_model_netting_proration_integration.py — DB-backed
coverage for the PR-A modelling change (ADR-021 convergence, 2026-07-17):

1. **Proration (mass conservation)** — a periodic forecast carrying a
   `time_span_start`/`time_span_end` and wired (à la the new
   `_wire_node_to_pi` multi-wire) to every daily PI bucket it overlaps must
   distribute its quantity WITHOUT creating or destroying demand: the sum of
   the buckets' outflows equals the forecast quantity (no more single-day
   lumping).
2. **Netting (GREATEST, never SUM)** — forecast and firm customer-order
   demand for the SAME bucket are not additive: outflow =
   ``GREATEST(fc, co) + dep`` (dependent/transfer demand stays additive).
   Headline case from the plan: CO 100 + forecast 80 on one bucket
   ⇒ outflow 100, not 180.

Every test runs on BOTH propagation flavours — the same two selected by
``OOTILS_ENGINE=sql|python`` in ``api/routers/events.py:
_build_propagation_engine`` — built explicitly here (the pattern of
``scripts/parity_sql_vs_python.py``) so the pinning is deterministic
whatever the ambient env var says:

- ``sql``    → ``SqlPropagationEngine`` (PROPAGATE_SQL's outflow_contribs /
  outflows_agg split + GREATEST in per_bucket);
- ``python`` → ``PropagationEngine`` (``_recompute_pi_node``'s
  fc_total/co_total/dep_total accumulation + ``max(fc, co) + dep``).

Isolation: every test seeds its OWN uuid4 scenario/item/location/series and
COMMITS (the propagation result must be read back deterministically). The
finalizer deactivates (``active = FALSE``) every committed node and edge —
never a DELETE/cascade — so later modules' propagation queries (all filtered
on ``active = TRUE``) can never see this module's seed. Rows without an
``active`` flag (scenarios, items, locations, calc_runs, shortages,
projection_series) are keyed by fresh UUIDs and scenario-scoped: inert by
construction, deliberately left in place.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from .conftest import requires_db

pytestmark = requires_db

# Parts-per-trillion tolerance for the non-divisible daily-rate cases:
# Python Decimal (28 sig. digits) vs Postgres numeric(50,28) division agree
# far below this — same tolerance as scripts/parity_sql_vs_python.py.
TOL = Decimal("1e-9")

ENGINE_FLAVORS = ("sql", "python")


# ---------------------------------------------------------------------------
# Seed helper — tracks everything committed so the finalizer can deactivate it
# ---------------------------------------------------------------------------


class _GraphSeeder:
    """Seeds a scenario + one-or-more (item, series, daily PI chain) and
    demand nodes, recording every node/edge id for the deactivation
    finalizer. All inserts go through the test's ``conn``; the caller (the
    fixture) owns commit and teardown."""

    def __init__(self, conn):
        self.conn = conn
        self.node_ids: list[UUID] = []
        self.edge_ids: list[UUID] = []
        self.scenario_id: UUID = uuid4()
        self.today: date = conn.execute("SELECT CURRENT_DATE AS d").fetchone()["d"]
        conn.execute(
            "INSERT INTO scenarios (scenario_id, name) VALUES (%s, %s)",
            (self.scenario_id, f"netting-proration-{self.scenario_id}"),
        )

    # -- structure ----------------------------------------------------------

    def series(self, buckets: int) -> tuple[UUID, UUID, UUID, list[UUID]]:
        """Create item + location + projection_series + `buckets` daily PI
        nodes chained by feeds_forward. Returns (item_id, location_id,
        series_id, pi_ids) with pi_ids indexed by bucket_sequence (bucket b
        covers [today+b, today+b+1))."""
        item_id, location_id, series_id = uuid4(), uuid4(), uuid4()
        self.conn.execute(
            "INSERT INTO items (item_id, name) VALUES (%s, %s)",
            (item_id, f"NETPRO-ITEM-{item_id}"),
        )
        self.conn.execute(
            "INSERT INTO locations (location_id, name) VALUES (%s, %s)",
            (location_id, f"NETPRO-LOC-{location_id}"),
        )
        self.conn.execute(
            """
            INSERT INTO projection_series
                (series_id, item_id, location_id, scenario_id, horizon_start, horizon_end)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (series_id, item_id, location_id, self.scenario_id,
             self.today, self.today + timedelta(days=buckets)),
        )
        pi_ids: list[UUID] = []
        for b in range(buckets):
            pi_id = uuid4()
            pi_ids.append(pi_id)
            self.node_ids.append(pi_id)
            self.conn.execute(
                """
                INSERT INTO nodes
                    (node_id, node_type, scenario_id, item_id, location_id,
                     time_grain, time_span_start, time_span_end,
                     projection_series_id, bucket_sequence, is_dirty, active)
                VALUES (%s, 'ProjectedInventory', %s, %s, %s,
                        'day', %s, %s, %s, %s, TRUE, TRUE)
                """,
                (pi_id, self.scenario_id, item_id, location_id,
                 self.today + timedelta(days=b), self.today + timedelta(days=b + 1),
                 series_id, b),
            )
            if b > 0:
                self._edge("feeds_forward", pi_ids[b - 1], pi_id)
        return item_id, location_id, series_id, pi_ids

    def demand(
        self,
        node_type: str,
        item_id: UUID,
        location_id: UUID,
        qty: int,
        pi_ids: list[UUID],
        *,
        day: int | None = None,
        span: tuple[int, int] | None = None,
    ) -> UUID:
        """Insert a demand node + its 'consumes' edge(s).

        ``day``  → point demand at today+day, wired to that single bucket.
        ``span`` → periodic demand over [today+a, today+b), wired to EVERY
        overlapped daily bucket — exactly what the new ``_wire_node_to_pi``
        produces for a forecast carrying a time_span. Mirrors the ingest
        contract: a periodic node carries BOTH the anchor time_ref (span
        start) and the span itself.
        """
        node_id = uuid4()
        self.node_ids.append(node_id)
        if span is not None:
            a, b = span
            time_ref = self.today + timedelta(days=a)
            span_start = self.today + timedelta(days=a)
            span_end = self.today + timedelta(days=b)
            wired = range(a, b)
        else:
            assert day is not None
            time_ref = self.today + timedelta(days=day)
            span_start = span_end = None
            wired = range(day, day + 1)
        self.conn.execute(
            """
            INSERT INTO nodes
                (node_id, node_type, scenario_id, item_id, location_id,
                 quantity, qty_uom, time_grain, time_ref,
                 time_span_start, time_span_end, is_dirty, active)
            VALUES (%s, %s, %s, %s, %s, %s, 'EA', %s, %s, %s, %s, FALSE, TRUE)
            """,
            (node_id, node_type, self.scenario_id, item_id, location_id,
             qty, "week" if span is not None else "exact_date",
             time_ref, span_start, span_end),
        )
        for b in wired:
            self._edge("consumes", node_id, pi_ids[b])
        return node_id

    def _edge(self, edge_type: str, from_id: UUID, to_id: UUID) -> None:
        edge_id = uuid4()
        self.edge_ids.append(edge_id)
        self.conn.execute(
            """
            INSERT INTO edges
                (edge_id, edge_type, from_node_id, to_node_id, scenario_id, active)
            VALUES (%s, %s, %s, %s, %s, TRUE)
            """,
            (edge_id, edge_type, from_id, to_id, self.scenario_id),
        )

    # -- teardown (deactivation, never delete) ------------------------------

    def deactivate_all(self) -> None:
        self.conn.rollback()  # drop any uncommitted leftovers first
        if self.edge_ids:
            self.conn.execute(
                "UPDATE edges SET active = FALSE WHERE edge_id = ANY(%s)",
                (self.edge_ids,),
            )
        if self.node_ids:
            self.conn.execute(
                "UPDATE nodes SET active = FALSE WHERE node_id = ANY(%s)",
                (self.node_ids,),
            )
        self.conn.commit()


@pytest.fixture
def graph(conn):
    seeder = _GraphSeeder(conn)
    yield seeder
    seeder.deactivate_all()


# ---------------------------------------------------------------------------
# Engine construction + propagation driver
# ---------------------------------------------------------------------------


def _build_engine(conn, flavor: str):
    """Explicit construction of the two OOTILS_ENGINE=sql|python flavours
    (same wiring as api/routers/events.py:_build_propagation_engine and
    scripts/parity_sql_vs_python.py)."""
    from ootils_core.engine.kernel.calc.projection import ProjectionKernel
    from ootils_core.engine.kernel.graph.dirty import DirtyFlagManager
    from ootils_core.engine.kernel.graph.store import GraphStore
    from ootils_core.engine.kernel.graph.traversal import GraphTraversal
    from ootils_core.engine.kernel.shortage.detector import ShortageDetector
    from ootils_core.engine.orchestration.calc_run import CalcRunManager
    from ootils_core.engine.orchestration.propagator import PropagationEngine
    from ootils_core.engine.orchestration.propagator_sql import SqlPropagationEngine

    cls = {"sql": SqlPropagationEngine, "python": PropagationEngine}[flavor]
    store = GraphStore(conn)
    return cls(
        store=store,
        traversal=GraphTraversal(store),
        dirty=DirtyFlagManager(),
        calc_run_mgr=CalcRunManager(),
        kernel=ProjectionKernel(),
        shortage_detector=ShortageDetector(),
    )


def _propagate(conn, flavor: str, scenario_id: UUID, pi_ids: list[UUID]) -> UUID:
    """Mark the PI set dirty for a fresh calc_run and run `_propagate` on the
    requested flavour. The calc_runs row is inserted directly (status
    'running') instead of via start_calc_run so no session-scoped advisory
    lock is left dangling on the shared test DB."""
    from ootils_core.engine.kernel.graph.dirty import DirtyFlagManager
    from ootils_core.models import CalcRun

    calc_run_id = uuid4()
    conn.execute(
        """
        INSERT INTO calc_runs (calc_run_id, scenario_id, status, is_full_recompute)
        VALUES (%s, %s, 'running', TRUE)
        """,
        (calc_run_id, scenario_id),
    )
    # Persist the dirty set through the production path — flush_to_postgres
    # also runs the mandatory post-bulk-INSERT ANALYZE (#455).
    dirty_mgr = DirtyFlagManager()
    dirty_mgr.mark_dirty(set(pi_ids), scenario_id, calc_run_id, conn)
    dirty_mgr.flush_to_postgres(calc_run_id, scenario_id, conn)

    calc_run = CalcRun(
        calc_run_id=calc_run_id,
        scenario_id=scenario_id,
        is_full_recompute=True,
        status="running",
    )
    engine = _build_engine(conn, flavor)
    engine._propagate(calc_run, set(pi_ids), conn)
    conn.execute(
        "UPDATE calc_runs SET status = 'completed', completed_at = now() "
        "WHERE calc_run_id = %s",
        (calc_run_id,),
    )
    conn.commit()
    return calc_run_id


def _outflows_by_bucket(conn, series_id: UUID) -> dict[int, Decimal]:
    rows = conn.execute(
        """
        SELECT bucket_sequence, outflows FROM nodes
        WHERE projection_series_id = %s AND active = TRUE
        ORDER BY bucket_sequence
        """,
        (series_id,),
    ).fetchall()
    return {
        int(r["bucket_sequence"]): Decimal(str(r["outflows"]))
        for r in rows
        if r["outflows"] is not None
    }


# ---------------------------------------------------------------------------
# 1. Proration — mass conservation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("flavor", ENGINE_FLAVORS)
def test_proration_mass_conservation(graph, conn, flavor):
    """A periodic forecast wired to every daily bucket it overlaps must
    conserve mass: Σ outflows over the buckets == the forecast quantity —
    the anti-lumping guarantee (previously the whole monthly quantity landed
    on ONE day).

    Two series: an evenly divisible quantity (70 over 7 days → exactly
    10/day, asserted bucket-by-bucket and exactly in total) and a
    non-divisible one (100 over 7 days → 100/7 per day, asserted within the
    cross-engine Decimal/numeric tolerance).
    """
    item_a, loc_a, series_a, pi_a = graph.series(10)
    graph.demand("ForecastDemand", item_a, loc_a, 70, pi_a, span=(2, 9))

    item_b, loc_b, series_b, pi_b = graph.series(10)
    graph.demand("ForecastDemand", item_b, loc_b, 100, pi_b, span=(1, 8))
    conn.commit()

    _propagate(conn, flavor, graph.scenario_id, pi_a + pi_b)

    # Even division: exact per-bucket rate, exact total.
    out_a = _outflows_by_bucket(conn, series_a)
    for b in range(10):
        expected = Decimal("10") if 2 <= b < 9 else Decimal("0")
        assert out_a[b] == expected, f"bucket {b}: {out_a[b]} != {expected}"
    assert sum(out_a.values()) == Decimal("70")

    # Non-divisible division: per-bucket within tolerance, total conserved.
    out_b = _outflows_by_bucket(conn, series_b)
    daily = Decimal("100") / Decimal("7")
    for b in range(1, 8):
        assert abs(out_b[b] - daily) <= TOL, f"bucket {b}: {out_b[b]} != ~{daily}"
    assert out_b[0] == Decimal("0")
    assert out_b[8] == Decimal("0")
    assert out_b[9] == Decimal("0")
    assert abs(sum(out_b.values()) - Decimal("100")) <= TOL, (
        f"mass not conserved: Σ outflows = {sum(out_b.values())}, forecast = 100"
    )


# ---------------------------------------------------------------------------
# 2. Netting — GREATEST(fc, co) + dep, never fc + co
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("flavor", ENGINE_FLAVORS)
def test_netting_co_and_forecast_same_bucket(graph, conn, flavor):
    """CO 100 and forecast 80 on the SAME bucket ⇒ outflow 100, not 180
    (the CO fulfils the forecast it was booked against — plan headline
    case). Symmetric direction on another bucket (forecast > CO), and
    dependent demand stays ADDITIVE on a third."""
    item_id, loc_id, series_id, pi_ids = graph.series(10)

    # Bucket 3 — CO dominates: max(80, 100) = 100.
    graph.demand("ForecastDemand", item_id, loc_id, 80, pi_ids, day=3)
    graph.demand("CustomerOrderDemand", item_id, loc_id, 100, pi_ids, day=3)
    # Bucket 6 — forecast dominates: max(50, 20) = 50.
    graph.demand("ForecastDemand", item_id, loc_id, 50, pi_ids, day=6)
    graph.demand("CustomerOrderDemand", item_id, loc_id, 20, pi_ids, day=6)
    # Bucket 8 — dependent demand ADDS on top of the netted value:
    # max(40, 0) + 15 = 55 (never max(40, 15)).
    graph.demand("ForecastDemand", item_id, loc_id, 40, pi_ids, day=8)
    graph.demand("DependentDemand", item_id, loc_id, 15, pi_ids, day=8)
    conn.commit()

    _propagate(conn, flavor, graph.scenario_id, pi_ids)

    out = _outflows_by_bucket(conn, series_id)
    assert out[3] == Decimal("100"), f"bucket 3: {out[3]} — CO 100 + fc 80 must net to 100, not 180"
    assert out[6] == Decimal("50"), f"bucket 6: {out[6]} — fc 50 + CO 20 must net to 50, not 70"
    assert out[8] == Decimal("55"), f"bucket 8: {out[8]} — fc 40 netted, dep 15 additive → 55"
    for b in (0, 1, 2, 4, 5, 7, 9):
        assert out[b] == Decimal("0"), f"bucket {b}: expected 0, got {out[b]}"


@pytest.mark.parametrize("flavor", ENGINE_FLAVORS)
def test_netting_applies_to_prorated_forecast(graph, conn, flavor):
    """Proration and netting compose: a periodic forecast (70 over 7 days →
    10/day on buckets 2..8) overlapped by a CO of 25 in bucket 5 nets
    per-bucket — bucket 5 outflow = max(10, 25) = 25, the other span
    buckets keep the forecast's 10. Total = 6×10 + 25 = 85 (neither the
    gross 70+25=95 nor the pooled max(70,25)=70): Truth A nets at the fine
    daily grain, which is exactly why Σ max ≥ max Σ keeps ADR-021's
    items(B) ⊆ items(A) inclusion alive."""
    item_id, loc_id, series_id, pi_ids = graph.series(10)

    graph.demand("ForecastDemand", item_id, loc_id, 70, pi_ids, span=(2, 9))
    graph.demand("CustomerOrderDemand", item_id, loc_id, 25, pi_ids, day=5)
    conn.commit()

    _propagate(conn, flavor, graph.scenario_id, pi_ids)

    out = _outflows_by_bucket(conn, series_id)
    assert out[5] == Decimal("25"), f"bucket 5: {out[5]} — CO 25 must beat the prorated fc 10"
    for b in (2, 3, 4, 6, 7, 8):
        assert out[b] == Decimal("10"), f"bucket {b}: {out[b]} — prorated fc 10 expected"
    assert sum(out.values()) == Decimal("85")
