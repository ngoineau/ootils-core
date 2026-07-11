"""
tests/integration/test_rust_parity_integration.py — DB-backed proof that the
Rust engine swap (`RustPropagationEngine`, ADR-016 / PR-C) does not change a
single computed number nor bypass shortage detection:

  1. Full parity on a seeded baseline graph: the SAME graph propagated by
     `SqlPropagationEngine` then by `RustPropagationEngine` must yield
     identical ProjectedInventory rows (opening/inflows/outflows/closing,
     has_shortage, shortage_qty) AND identical `shortages` rows (item,
     location, date, qty, severity_score, severity_class).
  2. The same parity on a FORK carrying an ADR-025 planning-param overlay
     (`scenario_planning_overrides.safety_stock_qty`): the Rust wrapper runs
     shortage detection through the SAME overlay-aware SHORTAGES_SQL on
     Python's session (propagator_rust.py), so a fork's override must be
     visible — and equally visible — through both engines. This is THE test
     that proves the engine swap cannot silently un-fork detection.
  3. The PR-C boundary-commit failure contract, end-to-end: a Rust failure
     AFTER `_propagate_via_rust`'s mid-request commit must leave
     calc_runs.status='failed' (durable), dirty_nodes intact (self-healing),
     the scenario advisory lock released (provable from a second session),
     and a subsequent run over the same event via `SqlPropagationEngine`
     must converge cleanly.

Engines are driven through `_propagate` (tests 1-2, the exact seeding/driving
pattern of test_param_overlay_propagation_integration.py and
scripts/parity_sql_vs_python.py) and through the full `process_event`
lifecycle (test 3 — the failure contract lives in the interplay with
process_event's `ROLLBACK TO SAVEPOINT propagation_start`).

The seed graph is fully deterministic (no RNG) and every quantity divides
evenly so Decimal/NUMERIC parity is exact; the 1e-12 tolerance mirrors
scripts/parity_sql_vs_python.py's documented rounding allowance.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg.rows import dict_row

ootils_kernel = pytest.importorskip(
    "ootils_kernel",
    reason="Rust kernel not built — run `maturin develop` in rust/ootils_kernel/",
)

# E402: deliberately imported AFTER the importorskip gate above.
from ootils_core.constants import BASELINE_SCENARIO_ID  # noqa: E402
from ootils_core.engine.kernel.calc.projection import ProjectionKernel  # noqa: E402
from ootils_core.engine.kernel.graph.dirty import DirtyFlagManager  # noqa: E402
from ootils_core.engine.kernel.graph.store import GraphStore  # noqa: E402
from ootils_core.engine.kernel.graph.traversal import GraphTraversal  # noqa: E402
from ootils_core.engine.kernel.shortage.detector import ShortageDetector  # noqa: E402
from ootils_core.engine.orchestration import propagator_rust  # noqa: E402
from ootils_core.engine.orchestration.calc_run import CalcRunManager  # noqa: E402
from ootils_core.engine.orchestration.propagator_rust import (  # noqa: E402
    RustPropagationEngine,
)
from ootils_core.engine.orchestration.propagator_sql import (  # noqa: E402
    SqlPropagationEngine,
)
from ootils_core.engine.scenario.param_overlay import set_param_override  # noqa: E402

from .conftest import requires_db  # noqa: E402

pytestmark = requires_db

BUCKETS = 10
SS_BASE = Decimal("20")
SS_OVERRIDE = Decimal("999")
# Same tolerance as scripts/parity_sql_vs_python.py — parts-per-trillion,
# ~12 orders of magnitude below any business-meaningful inventory value.
TOL = Decimal("1e-12")
RUST_LOGGER = "ootils_core.engine.orchestration.propagator_rust"


# ---------------------------------------------------------------------------
# Deterministic seed — 2 items x BUCKETS daily buckets, both shortage classes
# ---------------------------------------------------------------------------


def _seed_graph(conn, scenario_id: UUID) -> dict:
    """Seed a deterministic supply/demand graph into `scenario_id`.

    item A (safety stock SS_BASE=20 via item_planning_params):
        OH 50 @ b0, PO +30 @ d3, CustomerOrder -40 @ d1,
        Forecast 20 spread over d4..d8 (span 4 days -> exactly 5/day).
        Closings: 50,10,10,40,35,30,25,20,20,20 — never negative, dips below
        SS at b1/b2 -> 'below_safety_stock' rows.
    item B (NO planning params):
        OH 5 @ b0, PO +10 @ d2, CustomerOrder -25 @ d5.
        Closings: 5,5,15,15,15,-10,-10,-10,-10,-10 -> 'stockout' rows b5..b9.

    All quantities divide evenly (20/4=5) so both engines produce exact
    Decimals — parity is byte-comparable, not just tolerance-comparable.
    Every node carries a fresh location_id so snapshots/resets can be scoped
    to THIS seed even when several tests share the baseline scenario.
    """
    today = date.today()
    location_id = uuid4()
    conn.execute(
        "INSERT INTO locations (location_id, name) VALUES (%s, %s)",
        (location_id, f"RUST-PARITY-LOC-{uuid4()}"),
    )

    item_a, item_b = uuid4(), uuid4()
    conn.execute(
        "INSERT INTO items (item_id, name) VALUES (%s, %s), (%s, %s)",
        (item_a, f"RUST-PARITY-A-{uuid4()}", item_b, f"RUST-PARITY-B-{uuid4()}"),
    )

    # Safety stock for item A only — exercises BOTH ShortageDetector branches
    # (below_safety_stock on A, stockout on B whose ipp row is absent).
    conn.execute(
        """
        INSERT INTO item_planning_params
            (item_id, location_id, safety_stock_qty, effective_from, effective_to)
        VALUES (%s, %s, %s, %s, NULL)
        """,
        (item_a, location_id, SS_BASE, today),
    )

    pi_ids: dict[tuple[UUID, int], UUID] = {}
    oh_ids: dict[UUID, UUID] = {}
    for item_id, oh_qty in ((item_a, Decimal("50")), (item_b, Decimal("5"))):
        series_id = uuid4()
        conn.execute(
            """
            INSERT INTO projection_series
                (series_id, item_id, location_id, scenario_id, horizon_start, horizon_end)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (series_id, item_id, location_id, scenario_id,
             today, today + timedelta(days=BUCKETS)),
        )
        oh_id = uuid4()
        oh_ids[item_id] = oh_id
        conn.execute(
            """
            INSERT INTO nodes
                (node_id, node_type, scenario_id, item_id, location_id,
                 quantity, qty_uom, time_grain, time_ref, is_dirty, active)
            VALUES (%s, 'OnHandSupply', %s, %s, %s, %s, 'EA', 'exact_date', %s, FALSE, TRUE)
            """,
            (oh_id, scenario_id, item_id, location_id, oh_qty, today),
        )
        for b in range(BUCKETS):
            pi_id = uuid4()
            pi_ids[(item_id, b)] = pi_id
            conn.execute(
                """
                INSERT INTO nodes
                    (node_id, node_type, scenario_id, item_id, location_id,
                     time_grain, time_span_start, time_span_end,
                     projection_series_id, bucket_sequence, is_dirty, active)
                VALUES (%s, 'ProjectedInventory', %s, %s, %s,
                        'day', %s, %s, %s, %s, TRUE, TRUE)
                """,
                (pi_id, scenario_id, item_id, location_id,
                 today + timedelta(days=b), today + timedelta(days=b + 1),
                 series_id, b),
            )
        # feeds_forward chain + OH replenishes bucket 0
        _edge(conn, scenario_id, "replenishes", oh_id, pi_ids[(item_id, 0)])
        for b in range(1, BUCKETS):
            _edge(conn, scenario_id, "feeds_forward",
                  pi_ids[(item_id, b - 1)], pi_ids[(item_id, b)])

    def _supply(item_id: UUID, qty: str, day: int) -> None:
        node_id = uuid4()
        conn.execute(
            """
            INSERT INTO nodes
                (node_id, node_type, scenario_id, item_id, location_id,
                 quantity, qty_uom, time_grain, time_ref, is_dirty, active)
            VALUES (%s, 'PurchaseOrderSupply', %s, %s, %s, %s, 'EA', 'exact_date', %s, FALSE, TRUE)
            """,
            (node_id, scenario_id, item_id, location_id,
             Decimal(qty), today + timedelta(days=day)),
        )
        _edge(conn, scenario_id, "replenishes", node_id, pi_ids[(item_id, day)])

    def _point_demand(item_id: UUID, node_type: str, qty: str, day: int) -> None:
        node_id = uuid4()
        conn.execute(
            """
            INSERT INTO nodes
                (node_id, node_type, scenario_id, item_id, location_id,
                 quantity, qty_uom, time_grain, time_ref, is_dirty, active)
            VALUES (%s, %s, %s, %s, %s, %s, 'EA', 'exact_date', %s, FALSE, TRUE)
            """,
            (node_id, node_type, scenario_id, item_id, location_id,
             Decimal(qty), today + timedelta(days=day)),
        )
        _edge(conn, scenario_id, "consumes", node_id, pi_ids[(item_id, day)])

    def _span_demand(item_id: UUID, qty: str, day_from: int, day_to: int) -> None:
        """Span [day_from, day_to) — one consumes edge per overlapped bucket,
        mirroring what the ingest pipeline produces (parity_sql_vs_python)."""
        node_id = uuid4()
        conn.execute(
            """
            INSERT INTO nodes
                (node_id, node_type, scenario_id, item_id, location_id,
                 quantity, qty_uom, time_grain, time_span_start, time_span_end,
                 is_dirty, active)
            VALUES (%s, 'ForecastDemand', %s, %s, %s, %s, 'EA', 'exact_date',
                    %s, %s, FALSE, TRUE)
            """,
            (node_id, scenario_id, item_id, location_id, Decimal(qty),
             today + timedelta(days=day_from), today + timedelta(days=day_to)),
        )
        for b in range(day_from, day_to):
            _edge(conn, scenario_id, "consumes", node_id, pi_ids[(item_id, b)])

    _supply(item_a, "30", 3)
    _point_demand(item_a, "CustomerOrderDemand", "40", 1)
    _span_demand(item_a, "20", 4, 8)  # 4-day span, exactly 5/day

    _supply(item_b, "10", 2)
    _point_demand(item_b, "CustomerOrderDemand", "25", 5)

    conn.commit()
    return {
        "location_id": location_id,
        "item_a": item_a,
        "item_b": item_b,
        "oh_a": oh_ids[item_a],
        "today": today,
    }


def _edge(conn, scenario_id: UUID, edge_type: str, from_id: UUID, to_id: UUID) -> None:
    conn.execute(
        """
        INSERT INTO edges (edge_id, edge_type, from_node_id, to_node_id, scenario_id, active)
        VALUES (%s, %s, %s, %s, %s, TRUE)
        """,
        (uuid4(), edge_type, from_id, to_id, scenario_id),
    )


def _seed_fork(conn) -> UUID:
    return conn.execute(
        "INSERT INTO scenarios (scenario_id, name, is_baseline, status) "
        "VALUES (%s, %s, FALSE, 'active') RETURNING scenario_id",
        (uuid4(), f"rust-parity-fork-{uuid4()}"),
    ).fetchone()["scenario_id"]


# ---------------------------------------------------------------------------
# Engine drivers, reset, snapshots, parity diff
# ---------------------------------------------------------------------------


def _build_engine(cls, conn):
    store = GraphStore(conn)
    return cls(
        store=store,
        traversal=GraphTraversal(store),
        dirty=DirtyFlagManager(),
        calc_run_mgr=CalcRunManager(),
        kernel=ProjectionKernel(),
        shortage_detector=ShortageDetector(),
    )


def _propagate_all(engine, conn, scenario_id: UUID, location_id: UUID) -> UUID:
    """Mark this seed's PI buckets dirty, run `_propagate`, resolve stale
    shortages and close the run — same shape as
    test_param_overlay_propagation_integration._propagate_bucket, over the
    whole seeded subgraph."""
    pi_rows = conn.execute(
        """
        SELECT node_id FROM nodes
        WHERE scenario_id = %s AND location_id = %s
          AND node_type = 'ProjectedInventory' AND active = TRUE
        """,
        (scenario_id, location_id),
    ).fetchall()
    pi_ids = {UUID(str(r["node_id"])) for r in pi_rows}
    assert pi_ids, "seed produced no PI buckets?"

    calc_mgr = CalcRunManager()
    calc_run = calc_mgr.start_calc_run(scenario_id=scenario_id, event_ids=[], db=conn)
    assert calc_run is not None, "could not acquire advisory lock for scenario"
    dirty = DirtyFlagManager()
    dirty.mark_dirty(pi_ids, scenario_id, calc_run.calc_run_id, conn)
    dirty.flush_to_postgres(calc_run.calc_run_id, scenario_id, conn)

    engine._propagate(calc_run, pi_ids, conn)
    engine._shortage_detector.resolve_stale(
        scenario_id=scenario_id, calc_run_id=calc_run.calc_run_id, db=conn
    )
    conn.execute(
        "UPDATE calc_runs SET status = 'completed', completed_at = now() "
        "WHERE calc_run_id = %s",
        (calc_run.calc_run_id,),
    )
    conn.execute("SELECT pg_advisory_unlock(hashtext(%s)::bigint)", (str(scenario_id),))
    conn.commit()
    return calc_run.calc_run_id


def _reset_computed_state(conn, scenario_id: UUID, location_id: UUID) -> None:
    """Clear the computed PI fields + shortages for THIS seed so the second
    engine recomputes from the same blank slate (parity_sql_vs_python's
    _reset_pi_state, scoped by location)."""
    conn.execute(
        """
        UPDATE nodes
        SET opening_stock = NULL, inflows = NULL, outflows = NULL,
            closing_stock = NULL, has_shortage = FALSE, shortage_qty = 0,
            is_dirty = TRUE, last_calc_run_id = NULL
        WHERE node_type = 'ProjectedInventory'
          AND scenario_id = %s AND location_id = %s AND active = TRUE
        """,
        (scenario_id, location_id),
    )
    conn.execute("DELETE FROM dirty_nodes WHERE scenario_id = %s", (scenario_id,))
    conn.execute(
        "DELETE FROM shortages WHERE scenario_id = %s AND location_id = %s",
        (scenario_id, location_id),
    )
    conn.execute(
        "UPDATE calc_runs SET status = 'completed', completed_at = now() "
        "WHERE scenario_id = %s AND status = 'running'",
        (scenario_id,),
    )
    conn.commit()


def _snapshot_pis(conn, scenario_id: UUID, location_id: UUID) -> dict[UUID, dict]:
    rows = conn.execute(
        """
        SELECT node_id, opening_stock, inflows, outflows, closing_stock,
               has_shortage, shortage_qty
        FROM nodes
        WHERE node_type = 'ProjectedInventory'
          AND scenario_id = %s AND location_id = %s AND active = TRUE
        """,
        (scenario_id, location_id),
    ).fetchall()
    return {UUID(str(r["node_id"])): r for r in rows}


def _snapshot_shortages(conn, scenario_id: UUID, location_id: UUID) -> dict[UUID, dict]:
    rows = conn.execute(
        """
        SELECT pi_node_id, item_id, location_id, shortage_date,
               shortage_qty, severity_score, severity_class
        FROM shortages
        WHERE scenario_id = %s AND location_id = %s AND status = 'active'
        """,
        (scenario_id, location_id),
    ).fetchall()
    return {UUID(str(r["pi_node_id"])): r for r in rows}


def _assert_decimal_eq(a, b, *, node_id, field) -> None:
    assert (a is None) == (b is None), f"{node_id} {field}: {a!r} vs {b!r}"
    if a is None:
        return
    delta = abs(Decimal(str(a)) - Decimal(str(b)))
    assert delta <= TOL, f"{node_id} {field}: sql={a!r} rust={b!r} (delta {delta})"


def _assert_pi_parity(sql_snap: dict, rust_snap: dict) -> None:
    assert set(sql_snap) == set(rust_snap), (
        f"PI node sets diverge: only_sql={set(sql_snap) - set(rust_snap)} "
        f"only_rust={set(rust_snap) - set(sql_snap)}"
    )
    for nid, sql_row in sql_snap.items():
        rust_row = rust_snap[nid]
        for f in ("opening_stock", "inflows", "outflows", "closing_stock", "shortage_qty"):
            _assert_decimal_eq(sql_row[f], rust_row[f], node_id=nid, field=f)
        assert sql_row["has_shortage"] == rust_row["has_shortage"], (
            f"{nid} has_shortage: sql={sql_row['has_shortage']} rust={rust_row['has_shortage']}"
        )


def _assert_shortage_parity(sql_snap: dict, rust_snap: dict) -> None:
    assert set(sql_snap) == set(rust_snap), (
        f"shortage row sets diverge: only_sql={set(sql_snap) - set(rust_snap)} "
        f"only_rust={set(rust_snap) - set(sql_snap)}"
    )
    for nid, sql_row in sql_snap.items():
        rust_row = rust_snap[nid]
        for f in ("shortage_qty", "severity_score"):
            _assert_decimal_eq(sql_row[f], rust_row[f], node_id=nid, field=f)
        for f in ("item_id", "location_id", "shortage_date", "severity_class"):
            assert sql_row[f] == rust_row[f], (
                f"{nid} {f}: sql={sql_row[f]!r} rust={rust_row[f]!r}"
            )


def _run_rust(conn, scenario_id: UUID, location_id: UUID, caplog) -> UUID:
    """Run the Rust engine and PROVE the Rust hot path executed (its stats
    log line), guarding against a silent fall-through to the SQL path."""
    engine = _build_engine(RustPropagationEngine, conn)
    with caplog.at_level(logging.INFO, logger=RUST_LOGGER):
        calc_run_id = _propagate_all(engine, conn, scenario_id, location_id)
    assert any(
        "RustPropagationEngine" in rec.getMessage() for rec in caplog.records
    ), "the Rust hot path must actually run (not the SQL small-set fallback)"
    return calc_run_id


# ===========================================================================
# 1. Full parity on the baseline graph
# ===========================================================================


def test_rust_engine_matches_sql_engine_on_baseline(conn, monkeypatch, caplog):
    monkeypatch.setattr(propagator_rust, "RUST_DISPATCH_THRESHOLD", 0)
    seed = _seed_graph(conn, BASELINE_SCENARIO_ID)
    loc = seed["location_id"]

    sql_engine = _build_engine(SqlPropagationEngine, conn)
    _propagate_all(sql_engine, conn, BASELINE_SCENARIO_ID, loc)
    sql_pis = _snapshot_pis(conn, BASELINE_SCENARIO_ID, loc)
    sql_shortages = _snapshot_shortages(conn, BASELINE_SCENARIO_ID, loc)

    # Non-vacuity guards: the seed must produce work worth comparing —
    # every PI computed, and BOTH shortage classes present.
    assert len(sql_pis) == 2 * BUCKETS
    assert all(r["closing_stock"] is not None for r in sql_pis.values())
    classes = {r["severity_class"] for r in sql_shortages.values()}
    assert {"stockout", "below_safety_stock"} <= classes, (
        f"seed must produce both shortage classes, got {classes}"
    )

    _reset_computed_state(conn, BASELINE_SCENARIO_ID, loc)
    _run_rust(conn, BASELINE_SCENARIO_ID, loc, caplog)

    rust_pis = _snapshot_pis(conn, BASELINE_SCENARIO_ID, loc)
    rust_shortages = _snapshot_shortages(conn, BASELINE_SCENARIO_ID, loc)

    _assert_pi_parity(sql_pis, rust_pis)
    _assert_shortage_parity(sql_shortages, rust_shortages)


# ===========================================================================
# 2. Full parity on a fork carrying an ADR-025 safety-stock overlay override
# ===========================================================================


def test_rust_engine_matches_sql_engine_on_fork_with_param_overlay(
    conn, monkeypatch, caplog
):
    monkeypatch.setattr(propagator_rust, "RUST_DISPATCH_THRESHOLD", 0)
    fork = _seed_fork(conn)
    seed = _seed_graph(conn, fork)
    loc, item_a = seed["location_id"], seed["item_a"]
    set_param_override(
        conn, fork, item_a, "safety_stock_qty", str(SS_OVERRIDE),
        "rust-parity-test", location_id=loc,
    )
    conn.commit()

    def _assert_override_engaged(shortages: dict, engine_name: str) -> None:
        """With SS_OVERRIDE=999 >> any closing stock (max 50), EVERY item-A
        bucket must be below_safety_stock with qty > 900 — unreachable with
        the base SS of 20 (max qty would be 10). Detection through the base
        column instead of the overlay would fail this immediately, so parity
        alone can't mask a double blind spot."""
        a_rows = [r for r in shortages.values() if r["item_id"] == item_a]
        assert len(a_rows) == BUCKETS, (
            f"{engine_name}: override must put every item-A bucket in shortage "
            f"(got {len(a_rows)}/{BUCKETS})"
        )
        assert all(r["severity_class"] == "below_safety_stock" for r in a_rows)
        min_qty = min(Decimal(str(r["shortage_qty"])) for r in a_rows)
        assert min_qty > SS_BASE, (
            f"{engine_name}: shortage_qty={min_qty} is reachable with the BASE "
            f"safety stock — the fork's overlay override was not read"
        )

    sql_engine = _build_engine(SqlPropagationEngine, conn)
    _propagate_all(sql_engine, conn, fork, loc)
    sql_pis = _snapshot_pis(conn, fork, loc)
    sql_shortages = _snapshot_shortages(conn, fork, loc)
    _assert_override_engaged(sql_shortages, "SqlPropagationEngine")

    _reset_computed_state(conn, fork, loc)
    _run_rust(conn, fork, loc, caplog)

    rust_pis = _snapshot_pis(conn, fork, loc)
    rust_shortages = _snapshot_shortages(conn, fork, loc)
    _assert_override_engaged(rust_shortages, "RustPropagationEngine")

    _assert_pi_parity(sql_pis, rust_pis)
    _assert_shortage_parity(sql_shortages, rust_shortages)

    # Baseline purity: nothing about the fork run may leak shortage rows
    # into the baseline scenario for this seed's location.
    assert _snapshot_shortages(conn, BASELINE_SCENARIO_ID, loc) == {}


# ===========================================================================
# 3. Recovery after a Rust failure past the boundary commit
# ===========================================================================

_BOOM = "simulated rust failure after boundary commit (test)"


class _ExplodingKernel:
    """Stands in for ootils_kernel: fails like a Rust-session error would —
    AFTER `_propagate_via_rust` has already committed the request boundary."""

    @staticmethod
    def version() -> str:
        return "0.2.0"

    @staticmethod
    def propagate_and_write(dsn, password, calc_run_id_str, scenario_id_str):
        raise RuntimeError(_BOOM)


def test_rust_failure_after_boundary_commit_is_recoverable(
    conn, migrated_db, monkeypatch, caplog
):
    seed = _seed_graph(conn, BASELINE_SCENARIO_ID)
    loc, item_a = seed["location_id"], seed["item_a"]

    # A real event on item A's on-hand node, no dates -> full downstream
    # expansion (OH -> bucket0 -> feeds_forward chain).
    event_id = uuid4()
    conn.execute(
        """
        INSERT INTO events (event_id, event_type, scenario_id, trigger_node_id,
                            processed, source)
        VALUES (%s, 'supply_qty_changed', %s, %s, FALSE, 'test')
        """,
        (event_id, BASELINE_SCENARIO_ID, seed["oh_a"]),
    )
    conn.commit()

    monkeypatch.setattr(propagator_rust, "ootils_kernel", _ExplodingKernel())
    monkeypatch.setattr(propagator_rust, "RUST_DISPATCH_THRESHOLD", 0)
    engine = _build_engine(RustPropagationEngine, conn)

    password = conn.info.password
    with caplog.at_level(logging.DEBUG):
        with pytest.raises(RuntimeError, match="simulated rust failure"):
            engine.process_event(
                event_id=event_id, scenario_id=BASELINE_SCENARIO_ID, db=conn
            )
    # Discard process_event's redundant, uncommitted failure bookkeeping —
    # exactly what get_db's rollback-on-exception would do in production.
    conn.rollback()

    # Security contract of the PR-C fix: the DB password must never surface
    # in any log record of the failure path (DSN is built without it).
    if password:
        assert password not in caplog.text

    # (a) The failure record survived the rollback — it was made durable by
    # _fail_after_boundary_commit's own commit, not by the caller's.
    failed = conn.execute(
        """
        SELECT calc_run_id, status, error_message FROM calc_runs
        WHERE scenario_id = %s AND status = 'failed'
        ORDER BY created_at DESC LIMIT 1
        """,
        (BASELINE_SCENARIO_ID,),
    ).fetchone()
    assert failed is not None, (
        "calc_run must be durably 'failed' — a missing row means the "
        "'savepoint does not exist' pre-fix behaviour is back"
    )
    assert _BOOM in failed["error_message"]

    # (b) dirty_nodes intact for the failed run — the self-healing retry set.
    n_dirty = conn.execute(
        "SELECT COUNT(*) AS n FROM dirty_nodes WHERE calc_run_id = %s",
        (failed["calc_run_id"],),
    ).fetchone()["n"]
    assert n_dirty >= BUCKETS, (
        f"dirty_nodes must survive the failure (got {n_dirty}, "
        f"expected >= {BUCKETS} for item A's chain)"
    )

    # (c) The scenario advisory lock is RELEASED — provable only from a
    # second session (advisory locks are reentrant within one session).
    with psycopg.connect(migrated_db, row_factory=dict_row) as probe:
        got = probe.execute(
            "SELECT pg_try_advisory_lock(hashtext(%s)::bigint) AS locked",
            (str(BASELINE_SCENARIO_ID),),
        ).fetchone()["locked"]
        assert got is True, (
            "advisory lock still held after the failure — the scenario is "
            "stuck until the pooled connection recycles (the pre-fix bug)"
        )
        probe.execute(
            "SELECT pg_advisory_unlock(hashtext(%s)::bigint)",
            (str(BASELINE_SCENARIO_ID),),
        )

    # (d) The event was NOT consumed by the failed run.
    assert conn.execute(
        "SELECT processed FROM events WHERE event_id = %s", (event_id,)
    ).fetchone()["processed"] is False

    # (e) Re-run the SAME event through the SQL engine: clean convergence.
    sql_engine = _build_engine(SqlPropagationEngine, conn)
    rerun = sql_engine.process_event(
        event_id=event_id, scenario_id=BASELINE_SCENARIO_ID, db=conn
    )
    assert rerun is not None, "advisory lock must be acquirable for the re-run"
    conn.commit()
    assert rerun.status == "completed"

    # Item A's whole chain (the expanded subgraph) is computed and clean.
    stale = conn.execute(
        """
        SELECT COUNT(*) AS n FROM nodes
        WHERE scenario_id = %s AND location_id = %s AND item_id = %s
          AND node_type = 'ProjectedInventory' AND active = TRUE
          AND (closing_stock IS NULL OR is_dirty = TRUE)
        """,
        (BASELINE_SCENARIO_ID, loc, item_a),
    ).fetchone()["n"]
    assert stale == 0, f"{stale} item-A buckets left uncomputed/dirty after re-run"

    # The re-run cleaned up after itself and re-detected item A's shortages.
    assert conn.execute(
        "SELECT COUNT(*) AS n FROM dirty_nodes WHERE calc_run_id = %s",
        (rerun.calc_run_id,),
    ).fetchone()["n"] == 0
    n_shortages = conn.execute(
        """
        SELECT COUNT(*) AS n FROM shortages
        WHERE calc_run_id = %s AND location_id = %s AND status = 'active'
        """,
        (rerun.calc_run_id, loc),
    ).fetchone()["n"]
    assert n_shortages > 0, "re-run must re-detect item A's below-SS shortages"

    # And the event is finally consumed.
    assert conn.execute(
        "SELECT processed FROM events WHERE event_id = %s", (event_id,)
    ).fetchone()["processed"] is True
