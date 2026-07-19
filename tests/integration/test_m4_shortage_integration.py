"""
Integration tests for ootils_core.engine.kernel.shortage.detector
against a real PostgreSQL database.

Ported from tests/test_m4_shortage.py (which previously relied on MagicMock
for the `db` argument). The "no mocks" rule (CLAUDE.md) means every persisted
path is exercised by inserting real scenarios / items / locations / PI nodes /
calc_runs, calling ``ShortageDetector.detect`` + ``.persist`` (or
``resolve_stale`` / ``get_active_shortages``), and reading back the
``shortages`` rows.

Each test seeds its own data and cleans up at the end. The function-scoped
``conn`` fixture also rolls back uncommitted changes, but we use ``commit()``
inside tests so persisted rows can be queried back deterministically.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from ootils_core.engine.kernel._clock import FrozenClock
from ootils_core.engine.kernel._ids import deterministic_uuid
from ootils_core.engine.kernel.shortage.detector import ShortageDetector
from ootils_core.models import Node, ShortageRecord, Scenario

from .conftest import requires_db

pytestmark = requires_db


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------

def _insert_scenario(conn) -> UUID:
    scenario_id = uuid4()
    conn.execute(
        "INSERT INTO scenarios (scenario_id, name) VALUES (%s, %s)",
        (scenario_id, f"M4 Shortage Scenario {scenario_id}"),
    )
    return scenario_id


def _insert_item_and_location(conn) -> tuple[UUID, UUID]:
    item_id = uuid4()
    location_id = uuid4()
    conn.execute(
        "INSERT INTO items (item_id, name) VALUES (%s, %s)",
        (item_id, f"Shortage Test Item {item_id}"),
    )
    conn.execute(
        "INSERT INTO locations (location_id, name) VALUES (%s, %s)",
        (location_id, f"Shortage Test Loc {location_id}"),
    )
    return item_id, location_id


def _insert_calc_run(conn, scenario_id: UUID) -> UUID:
    calc_run_id = uuid4()
    conn.execute(
        """
        INSERT INTO calc_runs (calc_run_id, scenario_id, status, is_full_recompute)
        VALUES (%s, %s, 'completed', TRUE)
        """,
        (calc_run_id, scenario_id),
    )
    return calc_run_id


def _insert_pi_node(
    conn,
    *,
    scenario_id: UUID,
    item_id: UUID,
    location_id: UUID,
    closing_stock: Decimal | None,
    time_span_start: date = date(2026, 4, 1),
    time_span_end: date = date(2026, 4, 8),
    last_calc_run_id: UUID | None = None,
) -> UUID:
    """Insert a ProjectedInventory node.

    ``last_calc_run_id`` (chantier C3 « moteur d'exception », 2026-07-19):
    stamp the PI as recomputed by a given calc_run. ShortageDetector.resolve_stale
    now retires a shortage ONLY when its PI carries the current run's stamp
    (nodes.last_calc_run_id), so any direct-call resolve_stale fixture that
    expects a shortage to be resolved must simulate the recompute by stamping
    its PI. When set, the node is also made projection-balanced (opening_stock =
    closing_stock, inflows/outflows 0) so it satisfies the migration-087
    ``invariant_violations`` net, which scopes its balance/coherence laws to
    engine-stamped nodes (last_calc_run_id NOT NULL). When None (the default)
    the node is a pure closing-stock detection fixture with opening/inflows/
    outflows at 0 — NULL-stamped and out of the net's scope by design (see the
    migration-087 header)."""
    node_id = uuid4()
    # Balance the projection identity (closing = opening + inflows - outflows)
    # only for engine-stamped fixtures; NULL-stamped fixtures stay pure
    # closing-stock placeholders, deliberately out of the invariant net's scope.
    opening = closing_stock if last_calc_run_id is not None else Decimal("0")
    conn.execute(
        """
        INSERT INTO nodes (
            node_id, node_type, scenario_id, item_id, location_id,
            time_grain, time_span_start, time_span_end,
            closing_stock, opening_stock, inflows, outflows,
            has_shortage, shortage_qty, last_calc_run_id
        ) VALUES (
            %s, 'ProjectedInventory', %s, %s, %s,
            'day', %s, %s,
            %s, %s, 0, 0,
            %s, %s, %s
        )
        """,
        (
            node_id, scenario_id, item_id, location_id,
            time_span_start, time_span_end,
            closing_stock, opening,
            closing_stock is not None and closing_stock < 0,
            abs(closing_stock) if (closing_stock is not None and closing_stock < 0) else Decimal("0"),
            last_calc_run_id,
        ),
    )
    return node_id


def _make_pi_node_obj(
    *,
    node_id: UUID,
    scenario_id: UUID,
    item_id: UUID | None,
    location_id: UUID | None,
    closing_stock: Decimal | None,
    time_span_start: date | None = date(2026, 4, 1),
    time_span_end: date | None = date(2026, 4, 8),
) -> Node:
    """Build the in-memory Node passed to ShortageDetector.detect().

    `time_span_start` defaults to a non-None date so that
    ShortageDetector.persist() does not violate the
    shortages.shortage_date NOT NULL constraint. Tests that need to
    exercise the no-time-ref branch should pass time_span_start=None
    AND time_ref=None explicitly via Node, but persist() will then
    fail at the DB layer — by design.
    """
    return Node(
        node_id=node_id,
        node_type="ProjectedInventory",
        scenario_id=scenario_id,
        item_id=item_id,
        location_id=location_id,
        closing_stock=closing_stock,
        time_span_start=time_span_start,
        time_span_end=time_span_end,
        has_shortage=(closing_stock is not None and closing_stock < 0),
        shortage_qty=abs(closing_stock) if (closing_stock is not None and closing_stock < 0) else Decimal("0"),
    )


def _cleanup(
    conn,
    *,
    scenario_id: UUID,
    node_ids: list[UUID],
    calc_run_ids: list[UUID],
    item_id: UUID,
    location_id: UUID,
    drop_scenario: bool = True,
):
    """Delete every row written during the test so the DB stays clean."""
    if node_ids:
        conn.execute(
            "DELETE FROM shortages WHERE pi_node_id = ANY(%s)",
            (node_ids,),
        )
        conn.execute("DELETE FROM nodes WHERE node_id = ANY(%s)", (node_ids,))
    if calc_run_ids:
        # Defensive: any shortages still keyed by calc_run
        conn.execute(
            "DELETE FROM shortages WHERE calc_run_id = ANY(%s)",
            (calc_run_ids,),
        )
        conn.execute(
            "DELETE FROM calc_runs WHERE calc_run_id = ANY(%s)",
            (calc_run_ids,),
        )
    conn.execute("DELETE FROM locations WHERE location_id = %s", (location_id,))
    conn.execute("DELETE FROM items WHERE item_id = %s", (item_id,))
    if drop_scenario and scenario_id != Scenario.BASELINE_ID:
        conn.execute("DELETE FROM scenarios WHERE scenario_id = %s", (scenario_id,))
    conn.commit()


# ===========================================================================
# persist()
# ===========================================================================


class TestPersist:
    def test_persist_inserts_row(self, conn):
        scenario_id = _insert_scenario(conn)
        item_id, location_id = _insert_item_and_location(conn)
        calc_run_id = _insert_calc_run(conn, scenario_id)
        pi_id = _insert_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            closing_stock=Decimal("-50"),
        )
        conn.commit()
        try:
            frozen = FrozenClock(datetime(2026, 5, 23, 12, 0, 0, tzinfo=timezone.utc))
            detector = ShortageDetector(clock=frozen)
            pi_node = _make_pi_node_obj(
                node_id=pi_id, scenario_id=scenario_id,
                item_id=item_id, location_id=location_id,
                closing_stock=Decimal("-50"),
                time_span_start=date(2026, 4, 1),
                time_span_end=date(2026, 4, 8),
            )
            record = detector.detect(pi_node, calc_run_id, scenario_id, conn)
            assert record is not None
            detector.persist(record, conn)
            conn.commit()

            row = conn.execute(
                "SELECT * FROM shortages WHERE shortage_id = %s",
                (record.shortage_id,),
            ).fetchone()
            assert row is not None
            assert row["pi_node_id"] == pi_id
            assert row["scenario_id"] == scenario_id
            assert row["calc_run_id"] == calc_run_id
            assert Decimal(str(row["shortage_qty"])) == Decimal("50")
            assert Decimal(str(row["severity_score"])) == Decimal("350")  # 50 * 7
            assert row["status"] == "active"
            assert row["severity_class"] == "stockout"
            # FrozenClock should have set updated_at exactly
            assert row["updated_at"] == frozen.now()
        finally:
            _cleanup(conn, scenario_id=scenario_id, node_ids=[pi_id],
                     calc_run_ids=[calc_run_id],
                     item_id=item_id, location_id=location_id)

    def test_persist_is_idempotent_on_conflict(self, conn):
        """Persisting twice for the same (pi_node_id, calc_run_id) updates in place."""
        scenario_id = _insert_scenario(conn)
        item_id, location_id = _insert_item_and_location(conn)
        calc_run_id = _insert_calc_run(conn, scenario_id)
        pi_id = _insert_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            closing_stock=Decimal("-10"),
        )
        conn.commit()
        try:
            detector = ShortageDetector()
            pi_node = _make_pi_node_obj(
                node_id=pi_id, scenario_id=scenario_id,
                item_id=item_id, location_id=location_id,
                closing_stock=Decimal("-10"),
                time_span_start=date(2026, 4, 1),
                time_span_end=date(2026, 4, 8),
            )
            record_a = detector.detect(pi_node, calc_run_id, scenario_id, conn)
            detector.persist(record_a, conn)
            conn.commit()

            # Re-detect with larger shortage (simulating recompute)
            pi_node.closing_stock = Decimal("-25")
            pi_node.shortage_qty = Decimal("25")
            record_b = detector.detect(pi_node, calc_run_id, scenario_id, conn)
            detector.persist(record_b, conn)
            conn.commit()

            # Same deterministic shortage_id
            assert record_a.shortage_id == record_b.shortage_id

            # Only one row in shortages for this PI/calc_run
            rows = conn.execute(
                "SELECT * FROM shortages WHERE pi_node_id = %s AND calc_run_id = %s",
                (pi_id, calc_run_id),
            ).fetchall()
            assert len(rows) == 1
            assert Decimal(str(rows[0]["shortage_qty"])) == Decimal("25")
        finally:
            _cleanup(conn, scenario_id=scenario_id, node_ids=[pi_id],
                     calc_run_ids=[calc_run_id],
                     item_id=item_id, location_id=location_id)

    def test_persist_deterministic_shortage_id(self, conn):
        """shortage_id == deterministic_uuid('shortage', scenario, calc_run, node)."""
        scenario_id = _insert_scenario(conn)
        item_id, location_id = _insert_item_and_location(conn)
        calc_run_id = _insert_calc_run(conn, scenario_id)
        pi_id = _insert_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            closing_stock=Decimal("-7"),
        )
        conn.commit()
        try:
            detector = ShortageDetector()
            pi_node = _make_pi_node_obj(
                node_id=pi_id, scenario_id=scenario_id,
                item_id=item_id, location_id=location_id,
                closing_stock=Decimal("-7"),
            )
            record = detector.detect(pi_node, calc_run_id, scenario_id, conn)
            detector.persist(record, conn)
            conn.commit()

            expected = deterministic_uuid(
                "shortage", scenario_id, calc_run_id, pi_id,
            )
            assert record.shortage_id == expected
            row = conn.execute(
                "SELECT shortage_id FROM shortages WHERE pi_node_id = %s",
                (pi_id,),
            ).fetchone()
            assert row["shortage_id"] == expected
        finally:
            _cleanup(conn, scenario_id=scenario_id, node_ids=[pi_id],
                     calc_run_ids=[calc_run_id],
                     item_id=item_id, location_id=location_id)


# ===========================================================================
# get_active_shortages()
# ===========================================================================


class TestGetActiveShortages:
    def test_returns_empty_list_when_no_shortages(self, conn):
        scenario_id = _insert_scenario(conn)
        conn.commit()
        try:
            detector = ShortageDetector()
            result = detector.get_active_shortages(scenario_id, conn)
            assert result == []
        finally:
            conn.execute("DELETE FROM scenarios WHERE scenario_id = %s", (scenario_id,))
            conn.commit()

    def test_returns_list_of_shortage_records_sorted_desc(self, conn):
        scenario_id = _insert_scenario(conn)
        item_id, location_id = _insert_item_and_location(conn)
        calc_run_id = _insert_calc_run(conn, scenario_id)

        # Three PI nodes with different severities (qty × days)
        # n_low: -10 over 1 day → 10
        # n_mid: -10 over 7 days → 70
        # n_high: -100 over 7 days → 700
        n_low = _insert_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            closing_stock=Decimal("-10"),
            time_span_start=date(2026, 4, 1), time_span_end=date(2026, 4, 2),
        )
        n_mid = _insert_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            closing_stock=Decimal("-10"),
            time_span_start=date(2026, 4, 1), time_span_end=date(2026, 4, 8),
        )
        n_high = _insert_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            closing_stock=Decimal("-100"),
            time_span_start=date(2026, 4, 1), time_span_end=date(2026, 4, 8),
        )
        conn.commit()

        try:
            detector = ShortageDetector()
            for node_id, qty, end in (
                (n_low, Decimal("-10"), date(2026, 4, 2)),
                (n_mid, Decimal("-10"), date(2026, 4, 8)),
                (n_high, Decimal("-100"), date(2026, 4, 8)),
            ):
                pi = _make_pi_node_obj(
                    node_id=node_id, scenario_id=scenario_id,
                    item_id=item_id, location_id=location_id,
                    closing_stock=qty,
                    time_span_start=date(2026, 4, 1), time_span_end=end,
                )
                record = detector.detect(pi, calc_run_id, scenario_id, conn)
                detector.persist(record, conn)
            conn.commit()

            results = detector.get_active_shortages(scenario_id, conn)
            assert len(results) == 3
            assert all(isinstance(r, ShortageRecord) for r in results)
            scores = [r.severity_score for r in results]
            assert scores == sorted(scores, reverse=True)
            assert scores[0] == Decimal("700")
            assert scores[-1] == Decimal("10")
        finally:
            _cleanup(conn, scenario_id=scenario_id, node_ids=[n_low, n_mid, n_high],
                     calc_run_ids=[calc_run_id],
                     item_id=item_id, location_id=location_id)

    def test_filters_by_scenario_id(self, conn):
        """Shortages in scenario A should not appear when querying scenario B."""
        scenario_a = _insert_scenario(conn)
        scenario_b = _insert_scenario(conn)
        item_id, location_id = _insert_item_and_location(conn)
        calc_run_a = _insert_calc_run(conn, scenario_a)
        calc_run_b = _insert_calc_run(conn, scenario_b)
        pi_a = _insert_pi_node(
            conn, scenario_id=scenario_a, item_id=item_id, location_id=location_id,
            closing_stock=Decimal("-1"),
        )
        pi_b = _insert_pi_node(
            conn, scenario_id=scenario_b, item_id=item_id, location_id=location_id,
            closing_stock=Decimal("-2"),
        )
        conn.commit()
        try:
            detector = ShortageDetector()
            pi_a_obj = _make_pi_node_obj(
                node_id=pi_a, scenario_id=scenario_a,
                item_id=item_id, location_id=location_id,
                closing_stock=Decimal("-1"),
            )
            pi_b_obj = _make_pi_node_obj(
                node_id=pi_b, scenario_id=scenario_b,
                item_id=item_id, location_id=location_id,
                closing_stock=Decimal("-2"),
            )
            detector.persist(detector.detect(pi_a_obj, calc_run_a, scenario_a, conn), conn)
            detector.persist(detector.detect(pi_b_obj, calc_run_b, scenario_b, conn), conn)
            conn.commit()

            results_a = detector.get_active_shortages(scenario_a, conn)
            results_b = detector.get_active_shortages(scenario_b, conn)
            assert {r.pi_node_id for r in results_a} == {pi_a}
            assert {r.pi_node_id for r in results_b} == {pi_b}
        finally:
            # Both scenarios share item_id/location_id — clean nodes + calc_runs
            # from both, then drop shared item/location, then drop both scenarios.
            conn.execute(
                "DELETE FROM shortages WHERE pi_node_id = ANY(%s)",
                ([pi_a, pi_b],),
            )
            conn.execute("DELETE FROM nodes WHERE node_id = ANY(%s)", ([pi_a, pi_b],))
            conn.execute(
                "DELETE FROM calc_runs WHERE calc_run_id = ANY(%s)",
                ([calc_run_a, calc_run_b],),
            )
            conn.execute("DELETE FROM locations WHERE location_id = %s", (location_id,))
            conn.execute("DELETE FROM items WHERE item_id = %s", (item_id,))
            conn.execute(
                "DELETE FROM scenarios WHERE scenario_id = ANY(%s)",
                ([scenario_a, scenario_b],),
            )
            conn.commit()

    def test_only_active_status_returned(self, conn):
        """Resolved shortages should not appear."""
        scenario_id = _insert_scenario(conn)
        item_id, location_id = _insert_item_and_location(conn)
        calc_run_id = _insert_calc_run(conn, scenario_id)
        pi_id = _insert_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            closing_stock=Decimal("-5"),
        )
        conn.commit()
        try:
            detector = ShortageDetector()
            pi = _make_pi_node_obj(
                node_id=pi_id, scenario_id=scenario_id,
                item_id=item_id, location_id=location_id,
                closing_stock=Decimal("-5"),
            )
            record = detector.detect(pi, calc_run_id, scenario_id, conn)
            detector.persist(record, conn)
            conn.commit()

            assert len(detector.get_active_shortages(scenario_id, conn)) == 1

            # Mark as resolved manually
            conn.execute(
                "UPDATE shortages SET status = 'resolved' WHERE shortage_id = %s",
                (record.shortage_id,),
            )
            conn.commit()

            assert detector.get_active_shortages(scenario_id, conn) == []
        finally:
            _cleanup(conn, scenario_id=scenario_id, node_ids=[pi_id],
                     calc_run_ids=[calc_run_id],
                     item_id=item_id, location_id=location_id)


# ===========================================================================
# resolve_stale()
# ===========================================================================


class TestResolveStale:
    def test_resolves_shortages_from_other_calc_runs(self, conn):
        scenario_id = _insert_scenario(conn)
        item_id, location_id = _insert_item_and_location(conn)
        old_run = _insert_calc_run(conn, scenario_id)
        new_run = _insert_calc_run(conn, scenario_id)
        # Two PI nodes; one shortage from each calc_run. Both PIs are stamped
        # last_calc_run_id=new_run: chantier C3 scopes resolve_stale to the
        # series THIS run recomputed, so the "current run recomputed both PIs"
        # is exactly the situation that must retire the stale (old_run) shortage
        # while leaving the fresh (new_run) one active.
        pi_old = _insert_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            closing_stock=Decimal("-1"), last_calc_run_id=new_run,
        )
        pi_new = _insert_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            closing_stock=Decimal("-2"), last_calc_run_id=new_run,
        )
        conn.commit()
        try:
            detector = ShortageDetector()

            old_obj = _make_pi_node_obj(
                node_id=pi_old, scenario_id=scenario_id,
                item_id=item_id, location_id=location_id,
                closing_stock=Decimal("-1"),
            )
            new_obj = _make_pi_node_obj(
                node_id=pi_new, scenario_id=scenario_id,
                item_id=item_id, location_id=location_id,
                closing_stock=Decimal("-2"),
            )
            detector.persist(detector.detect(old_obj, old_run, scenario_id, conn), conn)
            detector.persist(detector.detect(new_obj, new_run, scenario_id, conn), conn)
            conn.commit()

            count = detector.resolve_stale(scenario_id, new_run, conn)
            conn.commit()
            assert count == 1

            # The "old" shortage should now be resolved; the "new" one still active.
            rows = conn.execute(
                "SELECT calc_run_id, status FROM shortages WHERE scenario_id = %s",
                (scenario_id,),
            ).fetchall()
            status_by_run = {r["calc_run_id"]: r["status"] for r in rows}
            assert status_by_run[old_run] == "resolved"
            assert status_by_run[new_run] == "active"
        finally:
            _cleanup(conn, scenario_id=scenario_id, node_ids=[pi_old, pi_new],
                     calc_run_ids=[old_run, new_run],
                     item_id=item_id, location_id=location_id)

    def test_returns_zero_when_nothing_to_resolve(self, conn):
        scenario_id = _insert_scenario(conn)
        calc_run_id = _insert_calc_run(conn, scenario_id)
        conn.commit()
        try:
            detector = ShortageDetector()
            count = detector.resolve_stale(scenario_id, calc_run_id, conn)
            conn.commit()
            assert count == 0
            assert isinstance(count, int)
        finally:
            conn.execute("DELETE FROM calc_runs WHERE calc_run_id = %s", (calc_run_id,))
            conn.execute("DELETE FROM scenarios WHERE scenario_id = %s", (scenario_id,))
            conn.commit()

    def test_does_not_resolve_shortages_in_other_scenarios(self, conn):
        scenario_a = _insert_scenario(conn)
        scenario_b = _insert_scenario(conn)
        item_id, location_id = _insert_item_and_location(conn)
        run_a = _insert_calc_run(conn, scenario_a)
        run_b = _insert_calc_run(conn, scenario_b)
        pi_a = _insert_pi_node(
            conn, scenario_id=scenario_a, item_id=item_id, location_id=location_id,
            closing_stock=Decimal("-1"),
        )
        pi_b = _insert_pi_node(
            conn, scenario_id=scenario_b, item_id=item_id, location_id=location_id,
            closing_stock=Decimal("-1"),
        )
        conn.commit()
        try:
            detector = ShortageDetector()
            for node_id, scen, run in (
                (pi_a, scenario_a, run_a),
                (pi_b, scenario_b, run_b),
            ):
                pi_obj = _make_pi_node_obj(
                    node_id=node_id, scenario_id=scen,
                    item_id=item_id, location_id=location_id,
                    closing_stock=Decimal("-1"),
                )
                detector.persist(detector.detect(pi_obj, run, scen, conn), conn)
            conn.commit()

            # A fresh, distinct run for scenario A that recomputes pi_a: stamp
            # it (chantier C3 scopes resolve_stale to series this run recomputed,
            # so pi_a's stale run_a shortage only retires once pi_a carries
            # new_run_a). Balance the PI in the same UPDATE (opening = closing)
            # to stay coherent with the migration-087 invariant net now that it
            # is engine-stamped.
            new_run_a = _insert_calc_run(conn, scenario_a)
            conn.execute(
                "UPDATE nodes SET last_calc_run_id = %s, opening_stock = closing_stock "
                "WHERE node_id = %s",
                (new_run_a, pi_a),
            )
            conn.commit()
            count = detector.resolve_stale(scenario_a, new_run_a, conn)
            conn.commit()
            # pi_a's shortage was generated by run_a (NOT the current run) and
            # pi_a WAS recomputed by new_run_a → it's stale → resolved. Count == 1.
            # scenario_b (pi_b) is untouched: resolve_stale is scenario-scoped.
            assert count == 1

            # But scenario B's shortage is untouched.
            row = conn.execute(
                "SELECT status FROM shortages WHERE scenario_id = %s",
                (scenario_b,),
            ).fetchone()
            assert row["status"] == "active"

        finally:
            # Shared item/location across both scenarios — clean in dependency order.
            try:
                conn.execute(
                    "DELETE FROM shortages WHERE pi_node_id = ANY(%s)",
                    ([pi_a, pi_b],),
                )
                conn.execute(
                    "DELETE FROM nodes WHERE node_id = ANY(%s)",
                    ([pi_a, pi_b],),
                )
                conn.execute(
                    "DELETE FROM calc_runs WHERE scenario_id = ANY(%s)",
                    ([scenario_a, scenario_b],),
                )
                conn.execute(
                    "DELETE FROM locations WHERE location_id = %s",
                    (location_id,),
                )
                conn.execute("DELETE FROM items WHERE item_id = %s", (item_id,))
                conn.execute(
                    "DELETE FROM scenarios WHERE scenario_id = ANY(%s)",
                    ([scenario_a, scenario_b],),
                )
                conn.commit()
            except Exception:
                conn.rollback()

    @pytest.mark.xfail(
        reason=(
            "DB trigger trg_shortages_updated_at (migration 016) "
            "overwrites updated_at = now() BEFORE UPDATE, so the "
            "Python-supplied frozen value never lands on UPDATE paths. "
            "This is a known limitation of chantier 6 clock injection "
            "for rows that pass through the trigger. Fix would be to "
            "either (a) drop the trigger and rely on the application "
            "layer's clock, or (b) make the trigger conditional on "
            "`NEW.updated_at IS NULL`. Out of scope for this PR."
        ),
        strict=False,
    )
    def test_uses_clock_for_updated_at(self, conn):
        """FrozenClock-supplied timestamp lands in shortages.updated_at."""
        scenario_id = _insert_scenario(conn)
        item_id, location_id = _insert_item_and_location(conn)
        old_run = _insert_calc_run(conn, scenario_id)
        new_run = _insert_calc_run(conn, scenario_id)
        # Stamp last_calc_run_id=new_run: chantier C3 only resolves a shortage
        # whose PI this run recomputed, so the count==1 precondition below holds
        # for the new-run recompute (the test's real subject is updated_at).
        pi_id = _insert_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            closing_stock=Decimal("-5"), last_calc_run_id=new_run,
        )
        conn.commit()
        try:
            frozen = FrozenClock(datetime(2026, 5, 23, 12, 0, 0, tzinfo=timezone.utc))
            detector = ShortageDetector(clock=frozen)

            pi_obj = _make_pi_node_obj(
                node_id=pi_id, scenario_id=scenario_id,
                item_id=item_id, location_id=location_id,
                closing_stock=Decimal("-5"),
            )
            detector.persist(detector.detect(pi_obj, old_run, scenario_id, conn), conn)
            conn.commit()

            count = detector.resolve_stale(scenario_id, new_run, conn)
            conn.commit()
            assert count == 1

            row = conn.execute(
                "SELECT status, updated_at FROM shortages WHERE pi_node_id = %s",
                (pi_id,),
            ).fetchone()
            assert row["status"] == "resolved"
            assert row["updated_at"] == frozen.now()
        finally:
            _cleanup(conn, scenario_id=scenario_id, node_ids=[pi_id],
                     calc_run_ids=[old_run, new_run],
                     item_id=item_id, location_id=location_id)
