"""
Integration tests for Pyramide axis A — PR3 (hierarchical reconciliation
runner + migration 054) against a real PostgreSQL database (no mocks).

Covered:
  - full hierarchical run over a synthetic seeded block with KNOWN weekly
    seasonal patterns per leaf (minimal D6-style generator below):
    per-level persistence is queryable (aggregates carry
    hierarchy_id/level/node_code, leaves carry item/location);
  - coherence: sum(leaf values) == block-root aggregate values per date
    (tolerance = NUMERIC(18,6) storage rounding);
  - recon_method persisted on every run of the block == the method
    EFFECTIVELY applied (migration 054 made the column real);
  - determinism: two identical runs produce identical stored quantities;
  - snapshots are leaf-only; commit materializes ONLY leaf series as
    ForecastDemand nodes; committing an aggregate run raises the typed
    guard (PyramideAggregateCommitError) and writes nothing;
  - a MinT request never lies: persisted recon_method equals the
    effective one whether the optional backend ran or fell back.

Conventions (mirrors test_pyramide_hierarchy_integration.py): the
function-scoped ``conn`` fixture (rollback teardown) keeps every test
self-cleaning; every test seeds its OWN registry rows / items (fresh
codes).
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from .conftest import requires_db

pytestmark = requires_db

BASELINE_SCENARIO_ID = UUID("00000000-0000-0000-0000-000000000001")
# Anti-flake rule (see test_pyramide_hierarchy_integration.py): seeded
# history stays at TODAY-4 .. TODAY-59 (strictly past even with a ±1-day
# client/server date skew); the forecast horizon starts at TODAY+2.
TODAY = date.today()

# Minimal D6-style synthetic generator: a KNOWN weekly pattern (7-day
# season) scaled per leaf. 8 full weeks => every weekday appears exactly
# 8 times, so per-leaf booked totals are amplitude * 14 * 8 and the
# middle-out shares are exactly 0.5 / 0.3 / 0.2.
WEEK_PATTERN = (2, 1, 1, 1, 3, 4, 2)  # sum = 14
AMPLITUDES = {"A1": 10, "A2": 6, "A3": 4}
HISTORY_DAYS = range(4, 60)  # 56 consecutive days = 8 full weeks


def _seed_block(conn, h: str, tag: str):
    """FAM-<tag> (family) -> PRD-<tag>1 (2 items) + PRD-<tag>2 (1 item)."""
    conn.execute(
        """
        INSERT INTO hierarchy (hierarchy_id, domain, scope, levels, is_default)
        VALUES (%s, 'product', 'local', %s, FALSE)
        """,
        (h, ["family", "product"]),
    )
    fam, prd1, prd2 = f"FAM-{tag}", f"PRD-{tag}1", f"PRD-{tag}2"
    for code, level, parent in [
        (fam, "family", None),
        (prd1, "product", fam),
        (prd2, "product", fam),
    ]:
        conn.execute(
            "INSERT INTO hierarchy_node (hierarchy_id, code, level, parent_code) "
            "VALUES (%s, %s, %s, %s)",
            (h, code, level, parent),
        )

    items: dict[str, UUID] = {}
    for suffix, leaf_code in [("A1", prd1), ("A2", prd1), ("A3", prd2)]:
        item_id = uuid4()
        ext = f"PR3-{tag}-{suffix}"
        conn.execute(
            "INSERT INTO items (item_id, name, item_type, uom, status, external_id) "
            "VALUES (%s, %s, 'finished_good', 'EA', 'active', %s)",
            (item_id, f"{ext} item", ext),
        )
        conn.execute(
            "INSERT INTO item_hierarchy (item_id, hierarchy_id, leaf_code) "
            "VALUES (%s, %s, %s)",
            (item_id, h, leaf_code),
        )
        items[suffix] = item_id
        for back in HISTORY_DAYS:
            day = TODAY - timedelta(days=back)
            qty = AMPLITUDES[suffix] * WEEK_PATTERN[day.weekday()]
            conn.execute(
                """
                INSERT INTO demand_history (
                    item_id, item_code, stream, booked_date,
                    ordered_quantity, value_ext, counts_for_asp,
                    fulfillment, order_number
                ) VALUES (%s, %s, 'regular', %s, %s, 0, FALSE, 'standard', 'PR3')
                """,
                (item_id, ext, day, qty),
            )

    loc_id = uuid4()
    conn.execute(
        "INSERT INTO locations (location_id, name, location_type, country, external_id) "
        "VALUES (%s, %s, 'dc', 'US', %s)",
        (loc_id, f"PR3-{tag} DC", f"PR3-{tag}-DC"),
    )
    return fam, prd1, prd2, items, loc_id


def _config(h: str, fam: str, loc_id: UUID, **overrides):
    from ootils_core.pyramide.hierarchy import HierarchicalRunConfig

    defaults = dict(
        hierarchy_id=h,
        block_code=fam,
        leaf_location_id=loc_id,
        scenario_id=BASELINE_SCENARIO_ID,
        horizon_start=TODAY + timedelta(days=2),
        horizon_days=14,
        granularity="daily",
        method="SEASONAL",
        method_params={"season_length": 7},
        lookback_days=90,
    )
    defaults.update(overrides)
    return HierarchicalRunConfig(**defaults)


def _values_by_forecast(conn, forecast_id: UUID) -> dict[date, Decimal]:
    rows = conn.execute(
        "SELECT forecast_date, quantity FROM forecast_values "
        "WHERE forecast_id = %s ORDER BY forecast_date ASC",
        (forecast_id,),
    ).fetchall()
    return {r["forecast_date"]: Decimal(str(r["quantity"])) for r in rows}


class TestHierarchicalRunPersistence:
    def test_full_run_persists_every_level_coherently(self, conn):
        from ootils_core.pyramide.hierarchy import HierarchicalRunner

        h = "pr3-h-full"
        fam, prd1, prd2, items, loc_id = _seed_block(conn, h, "F")
        result = HierarchicalRunner().run(conn, _config(h, fam, loc_id))

        assert result.recon_method == "middleout"
        assert result.recon_level == "family"
        by_key = {p.key: p for p in result.persisted}
        assert set(by_key) == {fam, prd1, prd2} | {str(i) for i in items.values()}

        # aggregates address hierarchy nodes; leaves address (item, loc)
        for code, level in [(fam, "family"), (prd1, "product"), (prd2, "product")]:
            row = conn.execute(
                "SELECT item_id, location_id, hierarchy_id, level, node_code "
                "FROM forecasts WHERE forecast_id = %s",
                (by_key[code].forecast_id,),
            ).fetchone()
            assert row["item_id"] is None and row["location_id"] is None
            assert (row["hierarchy_id"], row["level"], row["node_code"]) == (h, level, code)
        for item_id in items.values():
            row = conn.execute(
                "SELECT item_id, location_id, node_code FROM forecasts "
                "WHERE forecast_id = %s",
                (by_key[str(item_id)].forecast_id,),
            ).fetchone()
            assert row["item_id"] == item_id
            assert row["location_id"] == loc_id
            assert row["node_code"] is None

        # coherence per date: sum(leaves) == family aggregate == sum(products)
        fam_values = _values_by_forecast(conn, by_key[fam].forecast_id)
        assert len(fam_values) == 14
        leaf_sums: dict[date, Decimal] = {}
        for item_id in items.values():
            for day, qty in _values_by_forecast(conn, by_key[str(item_id)].forecast_id).items():
                leaf_sums[day] = leaf_sums.get(day, Decimal("0")) + qty
        tolerance = Decimal("0.0001")  # NUMERIC(18,6) storage rounding x 3 leaves
        for day, fam_qty in fam_values.items():
            assert abs(leaf_sums[day] - fam_qty) <= tolerance, day

        # middle-out shares are exactly the seeded 0.5 / 0.3 / 0.2
        shares = {s.leaf: s.share for s in result.shares}
        assert shares[str(items["A1"])] == Decimal("0.5")
        assert shares[str(items["A2"])] == Decimal("0.3")
        assert shares[str(items["A3"])] == Decimal("0.2")

        # recon_method is REAL: persisted on every run of the block
        run_ids = [p.run_id for p in result.persisted]
        rows = conn.execute(
            "SELECT run_id, recon_method, method, random_seed, code_version "
            "FROM pyramide_runs WHERE run_id = ANY(%s)",
            (run_ids,),
        ).fetchall()
        assert len(rows) == 6
        assert all(r["recon_method"] == "middleout" for r in rows)
        assert all(r["method"] == "SEASONAL" for r in rows)

        # snapshots are leaf-only
        snapshot_rows = conn.execute(
            "SELECT run_id FROM pyramide_snapshots WHERE run_id = ANY(%s)",
            (run_ids,),
        ).fetchall()
        leaf_run_ids = {p.run_id for p in result.persisted if p.kind == "leaf"}
        assert {r["run_id"] for r in snapshot_rows} == leaf_run_ids
        assert all(
            (p.snapshot_id is None) == (p.kind == "aggregate")
            for p in result.persisted
        )

    def test_aggregate_run_is_queryable_via_summary(self, conn):
        from ootils_core.pyramide.hierarchy import HierarchicalRunner
        from ootils_core.pyramide.repository import fetch_run_summary

        h = "pr3-h-summary"
        fam, _, _, _, loc_id = _seed_block(conn, h, "S")
        result = HierarchicalRunner().run(conn, _config(h, fam, loc_id))
        agg = next(p for p in result.persisted if p.key == fam)
        summary = fetch_run_summary(conn, agg.run_id)
        assert summary is not None
        assert summary.snapshot_id is None
        assert summary.node_code == fam
        assert summary.level == "family"
        assert summary.item_id is None
        assert summary.value_count == 14
        assert summary.recon_method == "middleout"

    def test_two_runs_are_deterministic(self, conn):
        from ootils_core.pyramide.hierarchy import HierarchicalRunner

        h = "pr3-h-determinism"
        fam, _, _, _, loc_id = _seed_block(conn, h, "D")
        first = HierarchicalRunner().run(conn, _config(h, fam, loc_id))
        second = HierarchicalRunner().run(conn, _config(h, fam, loc_id))
        by_key_first = {p.key: p.forecast_id for p in first.persisted}
        by_key_second = {p.key: p.forecast_id for p in second.persisted}
        assert set(by_key_first) == set(by_key_second)
        for key, forecast_id in by_key_first.items():
            assert _values_by_forecast(conn, forecast_id) == \
                _values_by_forecast(conn, by_key_second[key]), key


class TestCommitBoundary:
    def test_commit_materializes_leaves_only(self, conn):
        from ootils_core.pyramide.hierarchy import HierarchicalRunner
        from ootils_core.pyramide.repository import (
            PyramideAggregateCommitError,
            commit_run,
        )

        h = "pr3-h-commit"
        fam, _, _, items, loc_id = _seed_block(conn, h, "C")
        result = HierarchicalRunner().run(conn, _config(h, fam, loc_id))

        def forecast_demand_count():
            return conn.execute(
                "SELECT COUNT(*) AS n FROM nodes "
                "WHERE node_type = 'ForecastDemand' AND scenario_id = %s "
                "  AND location_id = %s",
                (BASELINE_SCENARIO_ID, loc_id),
            ).fetchone()["n"]

        # the aggregate guard fires BEFORE any write
        agg = next(p for p in result.persisted if p.kind == "aggregate")
        with pytest.raises(PyramideAggregateCommitError, match="aggregate"):
            commit_run(conn, agg.run_id)
        assert forecast_demand_count() == 0
        status = conn.execute(
            "SELECT status FROM pyramide_runs WHERE run_id = %s", (agg.run_id,)
        ).fetchone()["status"]
        assert status == "generated"  # aggregate run untouched

        # a leaf run commits normally
        leaf = next(
            p for p in result.persisted if p.key == str(items["A1"])
        )
        committed = commit_run(conn, leaf.run_id)
        assert committed is not None
        assert committed.demand_node_count == 14
        assert forecast_demand_count() == 14
        node = conn.execute(
            "SELECT item_id, location_id FROM nodes "
            "WHERE node_type = 'ForecastDemand' AND scenario_id = %s "
            "  AND location_id = %s LIMIT 1",
            (BASELINE_SCENARIO_ID, loc_id),
        ).fetchone()
        assert node["item_id"] == items["A1"]


class TestMintProvenanceNeverLies:
    def test_persisted_recon_method_is_the_effective_one(self, conn):
        """Request MinT: whether the optional backend runs or falls back,
        the persisted recon_method equals the effective method reported by
        the runner, and a fallback leaves an explicit warning."""
        from ootils_core.pyramide.hierarchy import HierarchicalRunner

        h = "pr3-h-mint"
        fam, _, _, _, loc_id = _seed_block(conn, h, "M")
        result = HierarchicalRunner().run(
            conn,
            _config(h, fam, loc_id, recon_method="mintrace_wls_shrink"),
        )
        assert result.recon_method in {"mintrace_wls_shrink", "middleout"}
        if result.recon_method == "middleout":
            assert any("fell back" in w or "skipped" in w for w in result.warnings)

        rows = conn.execute(
            "SELECT recon_method FROM pyramide_runs WHERE run_id = ANY(%s)",
            ([p.run_id for p in result.persisted],),
        ).fetchall()
        assert {r["recon_method"] for r in rows} == {result.recon_method}
