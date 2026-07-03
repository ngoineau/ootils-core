"""
Integration tests for Pyramide axis A — PR2 (migration 053 + hierarchy
readers) against a real PostgreSQL database (no mocks).

Covered:
  - migration 053 applies (nullable leaf columns + aggregate-node columns
    on forecasts / pyramide_runs);
  - the leaf-XOR-aggregate CHECK accepts leaf rows and aggregate rows,
    and rejects hybrid rows;
  - repository.get_historical_demand_by_node aggregates demand_history
    over the node's subtree with the SAME business filters as the leaf
    reader (shared predicate fragment);
  - the leaf list endpoint keeps working with aggregate rows present
    (WHERE item_id IS NOT NULL guard);
  - load_summing_blocks builds S from real 047 registry rows.

Conventions (mirrors test_demand_foundation_integration.py): module-scoped
migrated_db, autocommit _db_conn for seeding/teardown, every test creates
its OWN registry rows / items (fresh codes) and cleans up in a finally
block.
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from .conftest import requires_db

pytestmark = requires_db

BASELINE_SCENARIO_ID = UUID("00000000-0000-0000-0000-000000000001")
# The demand-history predicates compare booked_date to the SERVER's
# CURRENT_DATE, while TODAY is the client's date.today() — the two can
# differ by a day around midnight or across timezones. Anti-flake rule
# for every seeded row: strict-past rows at TODAY-2 or earlier, future
# rows at TODAY+2 or later; never TODAY / TODAY±1 on an assertion edge.
TODAY = date.today()


# ---------------------------------------------------------------------------
# Helpers — DB access, hierarchy seeding, teardown
# ---------------------------------------------------------------------------


def _db_conn(dsn):
    import psycopg
    from psycopg.rows import dict_row
    return psycopg.connect(dsn, row_factory=dict_row, autocommit=True)


def _create_item(conn, ext_id: str) -> UUID:
    item_id = uuid4()
    conn.execute(
        """
        INSERT INTO items (item_id, name, item_type, uom, status, external_id)
        VALUES (%s, %s, 'finished_good', 'EA', 'active', %s)
        """,
        (item_id, f"{ext_id} test item", ext_id),
    )
    return item_id


def _create_location(conn, ext_id: str) -> UUID:
    loc_id = uuid4()
    conn.execute(
        """
        INSERT INTO locations (location_id, name, location_type, country, external_id)
        VALUES (%s, %s, 'dc', 'US', %s)
        """,
        (loc_id, f"{ext_id} test DC", ext_id),
    )
    return loc_id


def _seed_hierarchy(conn, hierarchy_id: str, *, is_default: bool = False):
    """Two-level registry: FAM-* (family) -> PRD-* (product), per module
    docstring of tests/test_pyramide_hierarchy_summing.py."""
    conn.execute(
        """
        INSERT INTO hierarchy (hierarchy_id, domain, scope, levels, is_default)
        VALUES (%s, 'product', 'local', %s, %s)
        """,
        (hierarchy_id, ["family", "product"], is_default),
    )
    for code, level, parent in [
        ("FAM-A", "family", None),
        ("PRD-A1", "product", "FAM-A"),
        ("PRD-A2", "product", "FAM-A"),
        ("FAM-B", "family", None),
        ("PRD-B1", "product", "FAM-B"),
    ]:
        conn.execute(
            """
            INSERT INTO hierarchy_node (hierarchy_id, code, level, parent_code)
            VALUES (%s, %s, %s, %s)
            """,
            (hierarchy_id, code, level, parent),
        )


def _attach_item(conn, item_id: UUID, hierarchy_id: str, leaf_code: str):
    conn.execute(
        """
        INSERT INTO item_hierarchy (item_id, hierarchy_id, leaf_code)
        VALUES (%s, %s, %s)
        """,
        (item_id, hierarchy_id, leaf_code),
    )


def _insert_dh(
    conn,
    item_id: UUID,
    item_code: str,
    booked_date: date,
    qty,
    *,
    stream: str = "regular",
    fulfillment: str | None = "standard",
    warehouse_id: str | None = None,
):
    conn.execute(
        """
        INSERT INTO demand_history (
            item_id, item_code, stream, booked_date,
            ordered_quantity, value_ext, counts_for_asp,
            warehouse_id, fulfillment, order_number
        ) VALUES (%s, %s, %s, %s, %s, 0, FALSE, %s, %s, 'TEST-PH')
        """,
        (item_id, item_code, stream, booked_date, qty, warehouse_id, fulfillment),
    )


def _insert_leaf_forecast(conn, item_id: UUID, location_id: UUID) -> UUID:
    forecast_id = uuid4()
    conn.execute(
        """
        INSERT INTO forecasts (
            forecast_id, item_id, location_id, scenario_id,
            horizon_start, horizon_end, granularity, method
        ) VALUES (%s, %s, %s, %s, %s, %s, 'daily', 'MA')
        """,
        (forecast_id, item_id, location_id, BASELINE_SCENARIO_ID,
         TODAY, TODAY + timedelta(days=6)),
    )
    return forecast_id


def _insert_aggregate_forecast(conn, hierarchy_id: str, node_code: str,
                               level: str) -> UUID:
    forecast_id = uuid4()
    conn.execute(
        """
        INSERT INTO forecasts (
            forecast_id, hierarchy_id, level, node_code, scenario_id,
            horizon_start, horizon_end, granularity, method
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'monthly', 'MA')
        """,
        (forecast_id, hierarchy_id, level, node_code, BASELINE_SCENARIO_ID,
         TODAY, TODAY + timedelta(days=29)),
    )
    return forecast_id


def _cleanup_hierarchy(conn, hierarchy_id: str, item_ids, location_ids=()):
    """FK teardown order: runs -> forecasts -> facts -> memberships ->
    nodes -> hierarchy -> items/locations (RESTRICT everywhere)."""
    conn.execute(
        "DELETE FROM pyramide_runs WHERE hierarchy_id = %s", (hierarchy_id,)
    )
    conn.execute(
        "DELETE FROM forecast_values WHERE forecast_id IN "
        "(SELECT forecast_id FROM forecasts WHERE hierarchy_id = %s)",
        (hierarchy_id,),
    )
    conn.execute("DELETE FROM forecasts WHERE hierarchy_id = %s", (hierarchy_id,))
    for item_id in item_ids:
        conn.execute("DELETE FROM pyramide_runs WHERE item_id = %s", (item_id,))
        conn.execute(
            "DELETE FROM forecast_values WHERE forecast_id IN "
            "(SELECT forecast_id FROM forecasts WHERE item_id = %s)",
            (item_id,),
        )
        conn.execute("DELETE FROM forecasts WHERE item_id = %s", (item_id,))
        conn.execute("DELETE FROM demand_history WHERE item_id = %s", (item_id,))
        conn.execute("DELETE FROM item_hierarchy WHERE item_id = %s", (item_id,))
        conn.execute("DELETE FROM items WHERE item_id = %s", (item_id,))
    conn.execute("DELETE FROM hierarchy_node WHERE hierarchy_id = %s", (hierarchy_id,))
    conn.execute("DELETE FROM hierarchy WHERE hierarchy_id = %s", (hierarchy_id,))
    for loc_id in location_ids:
        conn.execute("DELETE FROM locations WHERE location_id = %s", (loc_id,))


# ---------------------------------------------------------------------------
# (1) Migration 053 — schema shape
# ---------------------------------------------------------------------------


class TestMigration053Applied:
    def test_columns_added_and_leaf_columns_nullable(self, migrated_db):
        with _db_conn(migrated_db) as conn:
            rows = conn.execute(
                """
                SELECT table_name, column_name, is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name IN ('forecasts', 'pyramide_runs')
                  AND column_name IN
                      ('item_id', 'location_id', 'hierarchy_id', 'level', 'node_code')
                """
            ).fetchall()
        shape = {(r["table_name"], r["column_name"]): r["is_nullable"] for r in rows}
        for table in ("forecasts", "pyramide_runs"):
            for column in ("item_id", "location_id", "hierarchy_id",
                           "level", "node_code"):
                assert shape.get((table, column)) == "YES", (
                    f"{table}.{column} missing or NOT NULL"
                )

    def test_check_constraints_present(self, migrated_db):
        with _db_conn(migrated_db) as conn:
            rows = conn.execute(
                """
                SELECT conname FROM pg_constraint
                WHERE conname IN (
                    'chk_forecasts_leaf_xor_aggregate',
                    'chk_pyramide_runs_leaf_xor_aggregate',
                    'fk_forecasts_hierarchy_node',
                    'fk_pyramide_runs_hierarchy_node'
                )
                """
            ).fetchall()
        assert {r["conname"] for r in rows} == {
            "chk_forecasts_leaf_xor_aggregate",
            "chk_pyramide_runs_leaf_xor_aggregate",
            "fk_forecasts_hierarchy_node",
            "fk_pyramide_runs_hierarchy_node",
        }


# ---------------------------------------------------------------------------
# (2) CHECK leaf XOR aggregate
# ---------------------------------------------------------------------------


class TestLeafXorAggregateCheck:
    def test_accepts_leaf_and_aggregate_rows(self, migrated_db):
        h = "ph-h-check-ok"
        with _db_conn(migrated_db) as conn:
            _seed_hierarchy(conn, h)
            item_id = _create_item(conn, "PH-CK-ITEM")
            loc_id = _create_location(conn, "PH-CK-LOC")
            try:
                leaf_fid = _insert_leaf_forecast(conn, item_id, loc_id)
                agg_fid = _insert_aggregate_forecast(conn, h, "FAM-A", "family")

                rows = conn.execute(
                    "SELECT forecast_id, item_id, node_code FROM forecasts "
                    "WHERE forecast_id IN (%s, %s) ORDER BY node_code NULLS FIRST",
                    (leaf_fid, agg_fid),
                ).fetchall()
                assert len(rows) == 2
                assert rows[0]["item_id"] == item_id and rows[0]["node_code"] is None
                assert rows[1]["item_id"] is None and rows[1]["node_code"] == "FAM-A"

                # An aggregate pyramide_run on the aggregate forecast
                conn.execute(
                    """
                    INSERT INTO pyramide_runs (
                        forecast_id, hierarchy_id, level, node_code, scenario_id,
                        horizon_start, horizon_end, granularity, method,
                        source_history_count
                    ) VALUES (%s, %s, 'family', 'FAM-A', %s, %s, %s,
                              'monthly', 'MA', 0)
                    """,
                    (agg_fid, h, BASELINE_SCENARIO_ID,
                     TODAY, TODAY + timedelta(days=29)),
                )
                run = conn.execute(
                    "SELECT item_id, location_id, node_code FROM pyramide_runs "
                    "WHERE forecast_id = %s",
                    (agg_fid,),
                ).fetchone()
                assert run["item_id"] is None
                assert run["location_id"] is None
                assert run["node_code"] == "FAM-A"
            finally:
                _cleanup_hierarchy(conn, h, [item_id], [loc_id])

    def test_rejects_hybrid_and_orphan_rows(self, migrated_db):
        import psycopg
        h = "ph-h-check-ko"
        with _db_conn(migrated_db) as conn:
            _seed_hierarchy(conn, h)
            item_id = _create_item(conn, "PH-KO-ITEM")
            loc_id = _create_location(conn, "PH-KO-LOC")
            try:
                # hybrid: leaf pair AND a node_code
                with pytest.raises(psycopg.errors.CheckViolation):
                    conn.execute(
                        """
                        INSERT INTO forecasts (
                            forecast_id, item_id, location_id,
                            hierarchy_id, level, node_code, scenario_id,
                            horizon_start, horizon_end, granularity, method
                        ) VALUES (%s, %s, %s, %s, 'family', 'FAM-A', %s,
                                  %s, %s, 'daily', 'MA')
                        """,
                        (uuid4(), item_id, loc_id, h, BASELINE_SCENARIO_ID,
                         TODAY, TODAY + timedelta(days=6)),
                    )
                # leaf carrying an orphan hierarchy_id (the composite FK
                # is MATCH SIMPLE and skips node_code-NULL rows, so the
                # CHECK must catch this)
                with pytest.raises(psycopg.errors.CheckViolation):
                    conn.execute(
                        """
                        INSERT INTO forecasts (
                            forecast_id, item_id, location_id, hierarchy_id,
                            scenario_id, horizon_start, horizon_end,
                            granularity, method
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'daily', 'MA')
                        """,
                        (uuid4(), item_id, loc_id, h, BASELINE_SCENARIO_ID,
                         TODAY, TODAY + timedelta(days=6)),
                    )
                # aggregate without level
                with pytest.raises(psycopg.errors.CheckViolation):
                    conn.execute(
                        """
                        INSERT INTO forecasts (
                            forecast_id, hierarchy_id, node_code, scenario_id,
                            horizon_start, horizon_end, granularity, method
                        ) VALUES (%s, %s, 'FAM-A', %s, %s, %s, 'daily', 'MA')
                        """,
                        (uuid4(), h, BASELINE_SCENARIO_ID,
                         TODAY, TODAY + timedelta(days=6)),
                    )
                # aggregate without hierarchy_id
                with pytest.raises(psycopg.errors.CheckViolation):
                    conn.execute(
                        """
                        INSERT INTO forecasts (
                            forecast_id, node_code, scenario_id,
                            horizon_start, horizon_end, granularity, method
                        ) VALUES (%s, 'FAM-A', %s, %s, %s, 'daily', 'MA')
                        """,
                        (uuid4(), BASELINE_SCENARIO_ID,
                         TODAY, TODAY + timedelta(days=6)),
                    )
                # neither leaf nor aggregate
                with pytest.raises(psycopg.errors.CheckViolation):
                    conn.execute(
                        """
                        INSERT INTO forecasts (
                            forecast_id, scenario_id,
                            horizon_start, horizon_end, granularity, method
                        ) VALUES (%s, %s, %s, %s, 'daily', 'MA')
                        """,
                        (uuid4(), BASELINE_SCENARIO_ID,
                         TODAY, TODAY + timedelta(days=6)),
                    )
                # aggregate pointing at a node absent from the registry
                with pytest.raises(psycopg.errors.ForeignKeyViolation):
                    conn.execute(
                        """
                        INSERT INTO forecasts (
                            forecast_id, hierarchy_id, level, node_code,
                            scenario_id, horizon_start, horizon_end,
                            granularity, method
                        ) VALUES (%s, %s, 'family', 'NO-SUCH-NODE', %s,
                                  %s, %s, 'daily', 'MA')
                        """,
                        (uuid4(), h, BASELINE_SCENARIO_ID,
                         TODAY, TODAY + timedelta(days=6)),
                    )
            finally:
                _cleanup_hierarchy(conn, h, [item_id], [loc_id])


# ---------------------------------------------------------------------------
# (3) get_historical_demand_by_node — subtree aggregation + business filters
# ---------------------------------------------------------------------------


class TestHistoricalDemandByNode:
    def test_node_read_equals_sum_of_subtree_leaves(self, migrated_db):
        """FAM-A subtree = items under PRD-A1 + PRD-A2, across ALL DCs;
        FAM-B's item and business-filtered rows never leak in."""
        from ootils_core.pyramide.repository import get_historical_demand_by_node

        h = "ph-h-reader"
        # d2 at TODAY-3 (not -1/-2): stays strictly past AND outside a
        # 1-day server window even with a ±1-day client/server date skew.
        d1, d2 = TODAY - timedelta(days=4), TODAY - timedelta(days=3)
        with _db_conn(migrated_db) as conn:
            _seed_hierarchy(conn, h)
            item_a1 = _create_item(conn, "PH-RD-A1")
            item_a2 = _create_item(conn, "PH-RD-A2")
            item_b1 = _create_item(conn, "PH-RD-B1")
            _attach_item(conn, item_a1, h, "PRD-A1")
            _attach_item(conn, item_a2, h, "PRD-A2")
            _attach_item(conn, item_b1, h, "PRD-B1")

            # counted (day d1): a1=10 (DC-1), a2=4 (DC-2 — cross-site sum)
            _insert_dh(conn, item_a1, "PH-RD-A1", d1, 10, warehouse_id="DC-1")
            _insert_dh(conn, item_a2, "PH-RD-A2", d1, 4, warehouse_id="DC-2")
            # counted (day d2): a1=6, NULL warehouse counts at node level
            _insert_dh(conn, item_a1, "PH-RD-A1", d2, 6, warehouse_id=None)
            # excluded: other family, warranty, inter-entity, and
            # non-past rows (TODAY+2/+3: still >= server CURRENT_DATE
            # even with a ±1-day client/server skew)
            _insert_dh(conn, item_b1, "PH-RD-B1", d1, 100)
            _insert_dh(conn, item_a1, "PH-RD-A1", d1, 100, stream="warranty")
            _insert_dh(conn, item_a1, "PH-RD-A1", d1, 100,
                       fulfillment="inter_entity")
            _insert_dh(conn, item_a1, "PH-RD-A1", TODAY + timedelta(days=2), 100)
            _insert_dh(conn, item_a1, "PH-RD-A1", TODAY + timedelta(days=3), 100)
            try:
                fam_a = get_historical_demand_by_node(
                    conn, hierarchy_id=h, node_code="FAM-A", lookback_days=30
                )
                assert fam_a == [Decimal("14"), Decimal("6")]

                # mid-level node: only its own subtree
                prd_a2 = get_historical_demand_by_node(
                    conn, hierarchy_id=h, node_code="PRD-A2", lookback_days=30
                )
                assert prd_a2 == [Decimal("4")]

                # lookback bound: a 1-day window sees nothing (window =
                # server yesterday only; d2 is 3 days back, outside even
                # with a ±1-day client/server date skew)
                assert get_historical_demand_by_node(
                    conn, hierarchy_id=h, node_code="FAM-A", lookback_days=1
                ) == []
            finally:
                _cleanup_hierarchy(conn, h, [item_a1, item_a2, item_b1])

    def test_unknown_node_fails_loudly(self, migrated_db):
        from ootils_core.pyramide.repository import get_historical_demand_by_node

        h = "ph-h-reader-ko"
        with _db_conn(migrated_db) as conn:
            _seed_hierarchy(conn, h)
            try:
                with pytest.raises(ValueError, match="NO-SUCH-NODE"):
                    get_historical_demand_by_node(
                        conn, hierarchy_id=h, node_code="NO-SUCH-NODE",
                        lookback_days=30,
                    )
            finally:
                _cleanup_hierarchy(conn, h, [])

    def test_leaf_reader_contract_intact(self, migrated_db):
        """get_historical_demand (leaf pair) still filters per site and
        keeps its exact series contract after the predicate factoring."""
        from ootils_core.pyramide.repository import get_historical_demand

        with _db_conn(migrated_db) as conn:
            item_id = _create_item(conn, "PH-LEAF-ITEM")
            loc_id = _create_location(conn, "PH-LEAF-LOC")
            _insert_dh(conn, item_id, "PH-LEAF-ITEM", TODAY - timedelta(days=4),
                       7, warehouse_id="PH-LEAF-LOC")
            # other DC and NULL DC leave the per-site series (unchanged
            # rule); dates kept at TODAY-3/-2, comfortably strict-past
            _insert_dh(conn, item_id, "PH-LEAF-ITEM", TODAY - timedelta(days=3),
                       100, warehouse_id="OTHER-DC")
            _insert_dh(conn, item_id, "PH-LEAF-ITEM", TODAY - timedelta(days=2),
                       100, warehouse_id=None)
            try:
                series = get_historical_demand(
                    db=conn, item_id=item_id, location_id=loc_id,
                    lookback_days=30,
                )
                assert series == [Decimal("7")]
            finally:
                conn.execute(
                    "DELETE FROM demand_history WHERE item_id = %s", (item_id,)
                )
                conn.execute("DELETE FROM items WHERE item_id = %s", (item_id,))
                conn.execute(
                    "DELETE FROM locations WHERE location_id = %s", (loc_id,)
                )


# ---------------------------------------------------------------------------
# (4) Summing blocks from real registry rows
# ---------------------------------------------------------------------------


class TestSummingBlocksFromDb:
    def test_load_summing_blocks_matches_registry(self, migrated_db):
        from ootils_core.pyramide.hierarchy import load_summing_blocks

        h = "ph-h-blocks"
        with _db_conn(migrated_db) as conn:
            _seed_hierarchy(conn, h, is_default=True)
            item_a1 = _create_item(conn, "PH-BK-A1")
            item_a2 = _create_item(conn, "PH-BK-A2")
            item_b1 = _create_item(conn, "PH-BK-B1")
            _attach_item(conn, item_a1, h, "PRD-A1")
            _attach_item(conn, item_a2, h, "PRD-A2")
            _attach_item(conn, item_b1, h, "PRD-B1")
            try:
                blocks = load_summing_blocks(conn, hierarchy_id=h)
                assert [b.block_code for b in blocks] == ["FAM-A", "FAM-B"]

                block_a = blocks[0]
                # columns ordered by (leaf_code, item): PRD-A1 < PRD-A2
                assert block_a.leaves == (str(item_a1), str(item_a2))
                # FAM-A row sums both columns; y = S.b holds
                assert block_a.rows[0] == (0, 1)
                y = block_a.multiply([Decimal("2"), Decimal("3")])
                assert y[0] == Decimal("5")

                # default-hierarchy resolution path (domain parameter)
                by_domain = load_summing_blocks(conn, domain="product")
                assert by_domain == blocks
            finally:
                _cleanup_hierarchy(conn, h, [item_a1, item_a2, item_b1])


# ---------------------------------------------------------------------------
# (5) Leaf API endpoints ignore aggregate rows
# ---------------------------------------------------------------------------


class TestLeafEndpointsIgnoreAggregates:
    def test_list_forecasts_excludes_aggregate_rows(self, migrated_db):
        h = "ph-h-api"
        os.environ["DATABASE_URL"] = migrated_db
        os.environ["OOTILS_API_TOKEN"] = "integration-test-token"

        from fastapi.testclient import TestClient
        from ootils_core.api.app import create_app
        from ootils_core.api.dependencies import get_db
        from ootils_core.db.connection import OotilsDB

        app = create_app()

        def override_db():
            db = OotilsDB(migrated_db)
            with db.conn() as c:
                yield c

        app.dependency_overrides[get_db] = override_db
        auth = {"Authorization": "Bearer integration-test-token"}

        with _db_conn(migrated_db) as conn:
            _seed_hierarchy(conn, h)
            item_id = _create_item(conn, "PH-API-ITEM")
            loc_id = _create_location(conn, "PH-API-LOC")
            leaf_fid = _insert_leaf_forecast(conn, item_id, loc_id)
            agg_fid = _insert_aggregate_forecast(conn, h, "FAM-A", "family")
        try:
            with TestClient(app) as client:
                # list: the aggregate row is filtered out, no 500
                resp = client.get("/v1/demand/forecast", headers=auth)
                assert resp.status_code == 200, resp.text
                ids = {f["forecast_id"] for f in resp.json()["forecasts"]}
                assert str(leaf_fid) in ids
                assert str(agg_fid) not in ids

                # get by id: leaf OK, aggregate 404s on the LEAF endpoint
                assert client.get(
                    f"/v1/demand/forecast/{leaf_fid}", headers=auth
                ).status_code == 200
                assert client.get(
                    f"/v1/demand/forecast/{agg_fid}", headers=auth
                ).status_code == 404
        finally:
            app.dependency_overrides.clear()
            with _db_conn(migrated_db) as conn:
                _cleanup_hierarchy(conn, h, [item_id], [loc_id])
