"""
test_sprint1.py — Sprint 1 validation tests.

Sections:
  1. Pure unit tests (no DB required) — projection kernel, dirty flags
  2. Integration tests (require DATABASE_URL env var) — full propagation
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest

from ootils_core.engine.kernel.calc.projection import ProjectionKernel
from ootils_core.engine.kernel.graph.dirty import DirtyFlagManager
from ootils_core.models import Node


# ===========================================================================
# Helpers
# ===========================================================================

def _make_node(**kwargs) -> Node:
    defaults = dict(
        node_id=uuid4(),
        node_type="ProjectedInventory",
        scenario_id=uuid4(),
    )
    defaults.update(kwargs)
    return Node(**defaults)


# ===========================================================================
# 1. ProjectionKernel — pure unit tests
# ===========================================================================


class TestApplyContributionRule:
    """apply_contribution_rule: point_in_bucket semantics."""

    def setup_method(self):
        self.kernel = ProjectionKernel()
        self.start = date(2025, 1, 1)
        self.end = date(2025, 1, 31)

    def test_date_within_bucket_returns_qty(self):
        qty = self.kernel.apply_contribution_rule(
            source_date=date(2025, 1, 15),
            source_qty=Decimal("50"),
            bucket_start=self.start,
            bucket_end=self.end,
        )
        assert qty == Decimal("50")

    def test_date_at_bucket_start_returns_qty(self):
        qty = self.kernel.apply_contribution_rule(
            source_date=self.start,
            source_qty=Decimal("100"),
            bucket_start=self.start,
            bucket_end=self.end,
        )
        assert qty == Decimal("100")

    def test_date_at_bucket_end_excluded(self):
        qty = self.kernel.apply_contribution_rule(
            source_date=self.end,
            source_qty=Decimal("100"),
            bucket_start=self.start,
            bucket_end=self.end,
        )
        assert qty == Decimal("0")

    def test_date_before_bucket_returns_zero(self):
        qty = self.kernel.apply_contribution_rule(
            source_date=date(2024, 12, 31),
            source_qty=Decimal("100"),
            bucket_start=self.start,
            bucket_end=self.end,
        )
        assert qty == Decimal("0")

    def test_date_after_bucket_returns_zero(self):
        qty = self.kernel.apply_contribution_rule(
            source_date=date(2025, 2, 1),
            source_qty=Decimal("100"),
            bucket_start=self.start,
            bucket_end=self.end,
        )
        assert qty == Decimal("0")

    def test_unknown_rule_raises(self):
        with pytest.raises(ValueError, match="Unknown contribution rule"):
            self.kernel.apply_contribution_rule(
                source_date=date(2025, 1, 15),
                source_qty=Decimal("50"),
                bucket_start=self.start,
                bucket_end=self.end,
                rule="proportional",
            )


class TestComputePiNode:
    """compute_pi_node: basic ledger logic."""

    def setup_method(self):
        self.kernel = ProjectionKernel()
        self.start = date(2025, 1, 1)
        self.end = date(2025, 2, 1)

    def test_simple_inflow_and_outflow(self):
        result = self.kernel.compute_pi_node(
            opening_stock=Decimal("100"),
            supply_events=[(date(2025, 1, 15), Decimal("50"))],
            demand_events=[(date(2025, 1, 20), Decimal("30"))],
            bucket_start=self.start,
            bucket_end=self.end,
        )
        assert result["opening_stock"] == Decimal("100")
        assert result["inflows"] == Decimal("50")
        assert result["outflows"] == Decimal("30")
        assert result["closing_stock"] == Decimal("120")
        assert result["has_shortage"] is False
        assert result["shortage_qty"] == Decimal("0")

    def test_shortage_detected(self):
        result = self.kernel.compute_pi_node(
            opening_stock=Decimal("10"),
            supply_events=[],
            demand_events=[(date(2025, 1, 5), Decimal("50"))],
            bucket_start=self.start,
            bucket_end=self.end,
        )
        assert result["has_shortage"] is True
        assert result["shortage_qty"] == Decimal("40")
        assert result["closing_stock"] == Decimal("-40")

    def test_supply_outside_bucket_ignored(self):
        result = self.kernel.compute_pi_node(
            opening_stock=Decimal("100"),
            supply_events=[(date(2025, 2, 5), Decimal("50"))],  # Outside bucket
            demand_events=[],
            bucket_start=self.start,
            bucket_end=self.end,
        )
        assert result["inflows"] == Decimal("0")
        assert result["closing_stock"] == Decimal("100")

    def test_demand_outside_bucket_ignored(self):
        result = self.kernel.compute_pi_node(
            opening_stock=Decimal("100"),
            supply_events=[],
            demand_events=[(date(2024, 12, 31), Decimal("50"))],  # Outside bucket
            bucket_start=self.start,
            bucket_end=self.end,
        )
        assert result["outflows"] == Decimal("0")
        assert result["closing_stock"] == Decimal("100")

    def test_multiple_supply_events(self):
        supply = [
            (date(2025, 1, 5), Decimal("20")),
            (date(2025, 1, 10), Decimal("30")),
            (date(2025, 2, 1), Decimal("50")),  # excluded (at bucket_end)
        ]
        result = self.kernel.compute_pi_node(
            opening_stock=Decimal("0"),
            supply_events=supply,
            demand_events=[],
            bucket_start=self.start,
            bucket_end=self.end,
        )
        assert result["inflows"] == Decimal("50")  # 20 + 30 only

    def test_no_shortage_exact_zero(self):
        result = self.kernel.compute_pi_node(
            opening_stock=Decimal("50"),
            supply_events=[],
            demand_events=[(date(2025, 1, 1), Decimal("50"))],
            bucket_start=self.start,
            bucket_end=self.end,
        )
        assert result["closing_stock"] == Decimal("0")
        assert result["has_shortage"] is False
        assert result["shortage_qty"] == Decimal("0")


# ===========================================================================
# 2. DirtyFlagManager — unit tests (no DB)
# ===========================================================================


class TestDirtyFlagManagerInMemory:
    """DirtyFlagManager: in-memory tracking without DB interaction."""

    def setup_method(self):
        self.dfm = DirtyFlagManager()
        self.scenario_id = uuid4()
        self.calc_run_id = uuid4()
        self.db = MagicMock()

    def test_mark_dirty_and_is_dirty(self):
        node_id = uuid4()
        self.dfm.mark_dirty({node_id}, self.scenario_id, self.calc_run_id, self.db)
        assert self.dfm.is_dirty(node_id, self.scenario_id, self.calc_run_id) is True

    def test_unmarked_node_is_not_dirty(self):
        node_id = uuid4()
        assert self.dfm.is_dirty(node_id, self.scenario_id, self.calc_run_id) is False

    def test_clear_dirty_removes_node(self):
        node_id = uuid4()
        self.dfm.mark_dirty({node_id}, self.scenario_id, self.calc_run_id, self.db)
        self.dfm.clear_dirty(node_id, self.scenario_id, self.calc_run_id, self.db)
        assert self.dfm.is_dirty(node_id, self.scenario_id, self.calc_run_id) is False

    def test_clear_dirty_calls_db_delete(self):
        node_id = uuid4()
        self.dfm.mark_dirty({node_id}, self.scenario_id, self.calc_run_id, self.db)
        self.dfm.clear_dirty(node_id, self.scenario_id, self.calc_run_id, self.db)
        self.db.execute.assert_called_once()
        call_sql = self.db.execute.call_args[0][0]
        assert "DELETE FROM dirty_nodes" in call_sql

    def test_mark_dirty_multiple_nodes(self):
        nodes = {uuid4(), uuid4(), uuid4()}
        self.dfm.mark_dirty(nodes, self.scenario_id, self.calc_run_id, self.db)
        for node_id in nodes:
            assert self.dfm.is_dirty(node_id, self.scenario_id, self.calc_run_id) is True

    def test_get_dirty_nodes_returns_set(self):
        nodes = {uuid4(), uuid4()}
        self.dfm.mark_dirty(nodes, self.scenario_id, self.calc_run_id, self.db)
        result = self.dfm.get_dirty_nodes(self.calc_run_id, self.scenario_id, self.db)
        assert result == nodes

    def test_different_calc_runs_isolated(self):
        node_id = uuid4()
        run1 = uuid4()
        run2 = uuid4()
        self.dfm.mark_dirty({node_id}, self.scenario_id, run1, self.db)
        assert self.dfm.is_dirty(node_id, self.scenario_id, run1) is True
        assert self.dfm.is_dirty(node_id, self.scenario_id, run2) is False

    def test_flush_to_postgres_calls_executemany(self):
        nodes = {uuid4(), uuid4()}
        self.dfm.mark_dirty(nodes, self.scenario_id, self.calc_run_id, self.db)
        self.dfm.flush_to_postgres(self.calc_run_id, self.scenario_id, self.db)
        self.db.executemany.assert_called_once()
        rows_inserted = self.db.executemany.call_args[0][1]
        assert len(rows_inserted) == 2

    def test_flush_empty_does_not_call_db(self):
        self.dfm.flush_to_postgres(self.calc_run_id, self.scenario_id, self.db)
        self.db.executemany.assert_not_called()

    def test_load_from_postgres_populates_memory(self):
        node_id = uuid4()
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, key: str(node_id)
        self.db.execute.return_value.fetchall.return_value = [mock_row]
        self.dfm.load_from_postgres(self.calc_run_id, self.scenario_id, self.db)
        assert self.dfm.is_dirty(node_id, self.scenario_id, self.calc_run_id) is True


# ===========================================================================
# 3. CalcRunManager — unit tests (lock held)
# ===========================================================================


class TestCalcRunManagerLock:
    """CalcRunManager: returns None when advisory lock is held."""

    def test_returns_none_when_locked(self):
        from ootils_core.engine.orchestration.calc_run import CalcRunManager

        mgr = CalcRunManager()
        db = MagicMock()

        # pg_try_advisory_lock returns False (lock held)
        lock_row = MagicMock()
        lock_row.__getitem__ = lambda self, key: False
        lock_row.get = lambda key, default=None: False if key == "locked" else default

        # Make fetchone() return a dict-like object
        lock_mock = {"locked": False}

        db.execute.return_value.fetchone.return_value = lock_mock

        scenario_id = uuid4()
        result = mgr.start_calc_run(scenario_id, [], db)

        assert result is None

    def test_returns_calc_run_when_lock_acquired(self):
        from ootils_core.engine.orchestration.calc_run import CalcRunManager

        mgr = CalcRunManager()
        db = MagicMock()

        scenario_id = uuid4()

        # Set up mock call sequence
        call_count = [0]

        def mock_execute(sql, params=None):
            call_count[0] += 1
            mock_result = MagicMock()
            sql.strip()

            if "pg_try_advisory_lock" in sql:
                mock_result.fetchone.return_value = {"locked": True}
            elif "SELECT event_id FROM events" in sql:
                mock_result.fetchall.return_value = []
            else:
                mock_result.fetchone.return_value = None
                mock_result.fetchall.return_value = []
            return mock_result

        db.execute.side_effect = mock_execute

        result = mgr.start_calc_run(scenario_id, [], db)

        assert result is not None
        assert result.scenario_id == scenario_id
        assert result.status == "running"


# ===========================================================================
# 4. Integration test — requires DATABASE_URL
# ===========================================================================

DATABASE_URL = os.environ.get("DATABASE_URL")
requires_db = pytest.mark.skipif(
    not DATABASE_URL,
    reason="DATABASE_URL not set — skipping DB integration tests",
)


@requires_db
def test_po_date_change_propagates_to_pi():
    """
    Integration test: PO date change propagates correctly through PI series.

    1. Setup: Item X, Location Y, baseline scenario
       - OnHandSupply: 100 units today
       - PurchaseOrderSupply: 50 units due at day+30
       - ProjectedInventory: daily buckets for 90 days
       - ForecastDemand: 2 units/day (as monthly bucket)
    2. Run initial projection
    3. Change PO due_date from day+30 to day+45 (inject event)
    4. Run propagation
    5. Assert:
       - PI at day+30 through day+44: no longer shows PO inflow
       - PI at day+45: shows PO inflow of 50 units
       - PI nodes before day+30: unchanged
       - All dirty flags cleared after propagation
    """
    import psycopg
    from psycopg.rows import dict_row

    from ootils_core.engine.kernel.graph.store import GraphStore
    from ootils_core.engine.kernel.graph.traversal import GraphTraversal
    from ootils_core.engine.kernel.graph.dirty import DirtyFlagManager
    from ootils_core.engine.orchestration.calc_run import CalcRunManager
    from ootils_core.engine.orchestration.propagator import PropagationEngine

    today = date.today()

    with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
        with conn.cursor():
            # ----------------------------------------------------------------
            # Setup fixtures
            # ----------------------------------------------------------------
            item_id = uuid4()
            location_id = uuid4()
            scenario_id = UUID("00000000-0000-0000-0000-000000000001")  # Baseline

            conn.execute(
                "INSERT INTO items (item_id, name) VALUES (%s, %s)",
                (item_id, "Item X"),
            )
            conn.execute(
                "INSERT INTO locations (location_id, name) VALUES (%s, %s)",
                (location_id, "Location Y"),
            )

            # Create projection series
            series_id = uuid4()
            conn.execute(
                """
                INSERT INTO projection_series (series_id, item_id, location_id, scenario_id,
                    horizon_start, horizon_end)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (series_id, item_id, location_id, scenario_id, today, today + timedelta(days=90)),
            )

            # OnHandSupply node
            oh_node_id = uuid4()
            conn.execute(
                """
                INSERT INTO nodes (node_id, node_type, scenario_id, item_id, location_id,
                    quantity, qty_uom, time_grain, time_ref)
                VALUES (%s, 'OnHandSupply', %s, %s, %s, 100, 'EA', 'exact_date', %s)
                """,
                (oh_node_id, scenario_id, item_id, location_id, today),
            )

            # PurchaseOrder node
            po_node_id = uuid4()
            po_due_date = today + timedelta(days=30)
            conn.execute(
                """
                INSERT INTO nodes (node_id, node_type, scenario_id, item_id, location_id,
                    quantity, qty_uom, time_grain, time_ref)
                VALUES (%s, 'PurchaseOrderSupply', %s, %s, %s, 50, 'EA', 'exact_date', %s)
                """,
                (po_node_id, scenario_id, item_id, location_id, po_due_date),
            )

            # ForecastDemand node (2 units/day for 90 days = 180 total)
            fd_node_id = uuid4()
            conn.execute(
                """
                INSERT INTO nodes (node_id, node_type, scenario_id, item_id, location_id,
                    quantity, qty_uom, time_grain, time_span_start, time_span_end)
                VALUES (%s, 'ForecastDemand', %s, %s, %s, 180, 'EA', 'month', %s, %s)
                """,
                (fd_node_id, scenario_id, item_id, location_id,
                 today, today + timedelta(days=90)),
            )

            # ProjectedInventory nodes: daily buckets for 90 days
            pi_node_ids: list[UUID] = []
            prev_pi_id: Optional[UUID] = None
            for i in range(90):
                bucket_s = today + timedelta(days=i)
                bucket_e = today + timedelta(days=i + 1)
                pi_id = uuid4()
                conn.execute(
                    """
                    INSERT INTO nodes (
                        node_id, node_type, scenario_id, item_id, location_id,
                        time_grain, time_span_start, time_span_end,
                        projection_series_id, bucket_sequence,
                        opening_stock, inflows, outflows, closing_stock
                    ) VALUES (%s, 'ProjectedInventory', %s, %s, %s,
                        'day', %s, %s, %s, %s,
                        0, 0, 0, 0)
                    """,
                    (pi_id, scenario_id, item_id, location_id,
                     bucket_s, bucket_e, series_id, i),
                )
                pi_node_ids.append(pi_id)

                # feeds_forward edge from previous PI to this one
                if prev_pi_id is not None:
                    edge_id = uuid4()
                    conn.execute(
                        """
                        INSERT INTO edges (edge_id, edge_type, from_node_id, to_node_id, scenario_id)
                        VALUES (%s, 'feeds_forward', %s, %s, %s)
                        """,
                        (edge_id, prev_pi_id, pi_id, scenario_id),
                    )

                # replenishes edge: OH supply → first PI bucket only
                if i == 0:
                    edge_id = uuid4()
                    conn.execute(
                        """
                        INSERT INTO edges (edge_id, edge_type, from_node_id, to_node_id, scenario_id)
                        VALUES (%s, 'replenishes', %s, %s, %s)
                        """,
                        (edge_id, oh_node_id, pi_id, scenario_id),
                    )

                # replenishes edge: PO → PI bucket at day+30
                if i == 30:
                    edge_id = uuid4()
                    conn.execute(
                        """
                        INSERT INTO edges (edge_id, edge_type, from_node_id, to_node_id, scenario_id)
                        VALUES (%s, 'replenishes', %s, %s, %s)
                        """,
                        (edge_id, po_node_id, pi_id, scenario_id),
                    )

                # consumes edge: ForecastDemand → each PI bucket
                edge_id = uuid4()
                conn.execute(
                    """
                    INSERT INTO edges (edge_id, edge_type, from_node_id, to_node_id, scenario_id)
                    VALUES (%s, 'consumes', %s, %s, %s)
                    """,
                    (edge_id, fd_node_id, pi_id, scenario_id),
                )

                prev_pi_id = pi_id

            conn.commit()

            # ----------------------------------------------------------------
            # Run initial projection (direct kernel calls, no event system)
            # ----------------------------------------------------------------
            store = GraphStore(conn)
            traversal = GraphTraversal(store)
            dirty_mgr = DirtyFlagManager()
            calc_run_mgr = CalcRunManager()
            kernel = ProjectionKernel()
            propagator = PropagationEngine(store, traversal, dirty_mgr, calc_run_mgr, kernel)

            # Compute all PI nodes in order
            opening = Decimal("0")
            for i, pi_id in enumerate(pi_node_ids):
                bucket_s = today + timedelta(days=i)
                bucket_e = today + timedelta(days=i + 1)

                # Supply: OH at day 0, PO at day+30
                supply = []
                if i == 0:
                    supply.append((today, Decimal("100")))  # OH
                if i == 30:
                    supply.append((po_due_date, Decimal("50")))  # PO

                # Demand: 2 units/day
                demand = [(bucket_s, Decimal("2"))]

                result = kernel.compute_pi_node(
                    opening_stock=opening,
                    supply_events=supply,
                    demand_events=demand,
                    bucket_start=bucket_s,
                    bucket_end=bucket_e,
                )
                conn.execute(
                    """
                    UPDATE nodes SET
                        opening_stock = %s, inflows = %s, outflows = %s,
                        closing_stock = %s, has_shortage = %s, shortage_qty = %s
                    WHERE node_id = %s
                    """,
                    (
                        result["opening_stock"], result["inflows"], result["outflows"],
                        result["closing_stock"], result["has_shortage"], result["shortage_qty"],
                        pi_id,
                    ),
                )
                opening = result["closing_stock"]

            conn.commit()

            # Verify initial state: PI at day+30 has inflow of 50
            pi_day30 = conn.execute(
                "SELECT * FROM nodes WHERE node_id = %s",
                (pi_node_ids[30],),
            ).fetchone()
            assert pi_day30["inflows"] == Decimal("50"), (
                f"Expected inflows=50 at day+30 initially, got {pi_day30['inflows']}"
            )

            # Save PI values before day+30 (should be unchanged after propagation)
            pre_propagation_values = {}
            for i in range(30):
                row = conn.execute(
                    "SELECT closing_stock FROM nodes WHERE node_id = %s",
                    (pi_node_ids[i],),
                ).fetchone()
                pre_propagation_values[pi_node_ids[i]] = row["closing_stock"]

            # ----------------------------------------------------------------
            # Step 3: Change PO due_date from day+30 to day+45
            # ----------------------------------------------------------------
            new_po_date = today + timedelta(days=45)

            # Update PO node
            conn.execute(
                "UPDATE nodes SET time_ref = %s WHERE node_id = %s",
                (new_po_date, po_node_id),
            )

            # Move the replenishes edge from PI[30] to PI[45]
            conn.execute(
                """
                UPDATE edges SET to_node_id = %s
                WHERE from_node_id = %s AND edge_type = 'replenishes'
                  AND to_node_id = %s
                """,
                (pi_node_ids[45], po_node_id, pi_node_ids[30]),
            )

            # Insert planning event
            event_id = uuid4()
            conn.execute(
                """
                INSERT INTO events (
                    event_id, event_type, scenario_id, trigger_node_id,
                    field_changed, old_date, new_date, source
                ) VALUES (%s, 'po_date_changed', %s, %s, 'time_ref', %s, %s, 'test')
                """,
                (event_id, scenario_id, po_node_id, po_due_date, new_po_date),
            )
            conn.commit()

            # ----------------------------------------------------------------
            # Step 4: Run propagation
            # ----------------------------------------------------------------
            calc_run = propagator.process_event(event_id, scenario_id, conn)
            conn.commit()

            assert calc_run is not None, "process_event returned None — lock not acquired?"

            # ----------------------------------------------------------------
            # Step 5: Assertions
            # ----------------------------------------------------------------

            # PI nodes before day+30: closing_stock unchanged
            for i in range(30):
                row = conn.execute(
                    "SELECT closing_stock FROM nodes WHERE node_id = %s",
                    (pi_node_ids[i],),
                ).fetchone()
                assert row["closing_stock"] == pre_propagation_values[pi_node_ids[i]], (
                    f"PI[{i}] closing_stock changed unexpectedly: "
                    f"before={pre_propagation_values[pi_node_ids[i]]}, after={row['closing_stock']}"
                )

            # PI at day+30 through day+44: no PO inflow
            for i in range(30, 45):
                row = conn.execute(
                    "SELECT inflows FROM nodes WHERE node_id = %s",
                    (pi_node_ids[i],),
                ).fetchone()
                assert row["inflows"] == Decimal("0"), (
                    f"PI[{i}] should have inflows=0 (PO moved), got {row['inflows']}"
                )

            # PI at day+45: shows PO inflow of 50
            row = conn.execute(
                "SELECT inflows FROM nodes WHERE node_id = %s",
                (pi_node_ids[45],),
            ).fetchone()
            assert row["inflows"] == Decimal("50"), (
                f"PI[45] should have inflows=50, got {row['inflows']}"
            )

            # All dirty flags cleared
            dirty_count = conn.execute(
                """
                SELECT COUNT(*) AS cnt FROM dirty_nodes
                WHERE calc_run_id = %s
                """,
                (calc_run.calc_run_id,),
            ).fetchone()
            assert dirty_count["cnt"] == 0, (
                f"Expected 0 dirty_nodes after propagation, found {dirty_count['cnt']}"
            )

            # Cleanup
            conn.execute("DELETE FROM dirty_nodes WHERE calc_run_id = %s", (calc_run.calc_run_id,))
            conn.execute("DELETE FROM calc_runs WHERE calc_run_id = %s", (calc_run.calc_run_id,))
            conn.execute("DELETE FROM events WHERE event_id = %s", (event_id,))
            conn.execute("DELETE FROM edges WHERE scenario_id = %s AND from_node_id IN ("
                         "SELECT node_id FROM nodes WHERE item_id = %s)", (scenario_id, item_id))
            conn.execute("DELETE FROM nodes WHERE item_id = %s", (item_id,))
            conn.execute("DELETE FROM projection_series WHERE series_id = %s", (series_id,))
            conn.execute("DELETE FROM locations WHERE location_id = %s", (location_id,))
            conn.execute("DELETE FROM items WHERE item_id = %s", (item_id,))
            conn.commit()
