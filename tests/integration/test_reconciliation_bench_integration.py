"""
Integration tests for the reconciliation bench (Pyramide axis A, design §8)
against a real PostgreSQL database (no mocks).

Covered:
  - the bench runs end-to-end on one seeded block (synthetic KNOWN weekly
    patterns, same D6-style generator as
    test_pyramide_reconcile_integration.py, but with a history long enough
    to carve out a holdout window) and produces FINITE wape/mase/bias for
    'middleout' at all three levels (leaf, recon level, block root);
  - grain='week' on the same seed: cutoff snaps to an ISO Monday, the 4
    holdout weeks are complete (n_obs = n_series x 4), wape/bias finite
    (mase None: the weekly sums of the seed are constant);
  - determinism: two identical calls (fixed ``today``) return equal
    BenchReports and the same non-None verdict;
  - anti-leak: a demand line inserted INSIDE the holdout window (on the
    cutoff day itself — the boundary belongs to EVAL) does NOT change the
    training histories fed to the forecast engine (hence not the
    forecasts), only the actuals/metrics. Verified with a recording
    subclass of the REAL engine (instrumentation, not a mock: the real
    compute runs).

Conventions (mirrors test_pyramide_reconcile_integration.py): the
function-scoped ``conn`` fixture (rollback teardown) keeps every test
self-cleaning; every test seeds its OWN registry rows / items. Dates are
FIXED (the bench never uses CURRENT_DATE and takes an explicit ``today``),
so there is no wall-clock flakiness at all.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

from ootils_core.pyramide.engines import PyramideForecastEngine

from .conftest import requires_db

pytestmark = requires_db

# Fully fixed clock: the bench takes an explicit `today` and its read path
# composes the stream predicates with a parameterized window (never
# CURRENT_DATE), so seeding at fixed past dates is exactly the production
# replay situation.
FIXED_TODAY = date(2026, 3, 2)
HOLDOUT_DAYS = 28
HORIZON = 28
LOOKBACK_DAYS = 120
CUTOFF = FIXED_TODAY - timedelta(days=HOLDOUT_DAYS)

# D6-style synthetic generator (same pattern as
# test_pyramide_reconcile_integration.py): a KNOWN weekly pattern scaled
# per leaf. 12 train weeks + 4 holdout weeks of the SAME pattern, so every
# method has both a learnable signal and non-zero holdout actuals.
WEEK_PATTERN = (2, 1, 1, 1, 3, 4, 2)
AMPLITUDES = {"A1": 10, "A2": 6, "A3": 4}
TRAIN_DAYS = 84   # 12 full weeks strictly before the cutoff
SEEDED_DAYS = range(1, TRAIN_DAYS + HOLDOUT_DAYS + 1)  # FIXED_TODAY-1 .. cutoff-84


def _seed_block(conn, h: str, tag: str):
    """FAM-<tag> (family, root) -> PRD-<tag>1 (2 items) + PRD-<tag>2 (1 item).

    The hierarchy is registered as the domain default (the bench resolves
    by domain); any pre-existing 'product' default is unset INSIDE the
    test transaction — the conn fixture's rollback restores it.
    """
    conn.execute("UPDATE hierarchy SET is_default = FALSE WHERE domain = 'product'")
    conn.execute(
        """
        INSERT INTO hierarchy (hierarchy_id, domain, scope, levels, is_default)
        VALUES (%s, 'product', 'local', %s, TRUE)
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
        ext = f"BENCH-{tag}-{suffix}"
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
        for back in SEEDED_DAYS:
            day = FIXED_TODAY - timedelta(days=back)
            _insert_demand(
                conn, item_id, ext, day,
                AMPLITUDES[suffix] * WEEK_PATTERN[day.weekday()],
            )
    return fam, items


def _insert_demand(conn, item_id: UUID, item_code: str, day: date, qty) -> None:
    conn.execute(
        """
        INSERT INTO demand_history (
            item_id, item_code, stream, booked_date,
            ordered_quantity, value_ext, counts_for_asp,
            fulfillment, order_number
        ) VALUES (%s, %s, 'regular', %s, %s, 0, FALSE, 'standard', 'BENCH')
        """,
        (item_id, item_code, day, qty),
    )


def _run(conn, engine=None):
    from ootils_core.pyramide.hierarchy.bench import run_reconciliation_bench

    return run_reconciliation_bench(
        conn,
        domain="product",
        recon_level="product",
        lookback_days=LOOKBACK_DAYS,
        horizon=HORIZON,
        holdout_days=HOLDOUT_DAYS,
        methods=("middleout",),
        forecast_engine=engine,
        today=FIXED_TODAY,
    )


class _RecordingEngine(PyramideForecastEngine):
    """REAL engine + a record of every training history it was given.

    Instrumentation, not a mock: forecasts are computed by the production
    engine — the recording only makes the train split observable, which is
    what the anti-leak test asserts on.
    """

    def __init__(self):
        super().__init__()
        self.histories: list[tuple[Decimal, ...]] = []

    def forecast(self, **kwargs):
        self.histories.append(tuple(kwargs["history"]))
        return super().forecast(**kwargs)


class TestReconciliationBench:
    def test_middleout_scores_finitely_at_three_levels(self, conn):
        from ootils_core.pyramide.hierarchy.bench import (
            LEVEL_LEAF,
            LEVEL_ROOT,
            METHOD_BASE,
        )

        fam, items = _seed_block(conn, "bench-h-3lvl", "B1")
        report = _run(conn)

        assert report.domain == "product"
        assert report.cutoff == CUTOFF
        assert report.warnings == ()

        by_cell = {(r.level, r.method): r for r in report.rows}
        # middleout + the implicit 'base' comparator, each at 3 levels
        assert set(by_cell) == {
            (level, method)
            for level in (LEVEL_LEAF, "product", LEVEL_ROOT)
            for method in ("middleout", METHOD_BASE)
        }
        for level, n_series in [(LEVEL_LEAF, 3), ("product", 2), (LEVEL_ROOT, 1)]:
            row = by_cell[(level, "middleout")]
            assert row.block == fam
            assert row.n_series == n_series
            assert row.n_obs == n_series * HORIZON
            # FINITE metrics: holdout has booked demand (wape/bias defined)
            # and the weekly train pattern is non-constant (mase defined).
            assert row.wape is not None and row.wape >= 0, level
            assert row.mase is not None and row.mase >= 0, level
            assert row.bias is not None, level

    def test_empty_history_node_does_not_abort_the_block(self, conn):
        """A reconciliation node with no training-window demand must NOT
        drop the whole family (regression: found on real pilot data — a
        single dead group killed every sibling; the synthetic seed always
        had data for every node so the unit path missed it). The empty
        subtree forecasts zero + warns, siblings still score."""
        from ootils_core.pyramide.hierarchy.bench import LEVEL_LEAF

        # Normal block FAM-B4 -> PRD-B41 (2 items) + PRD-B42 (1 item),
        # then add a THIRD product node whose item only sells INSIDE the
        # holdout (nothing in the training window before the cutoff).
        fam, _ = _seed_block(conn, "bench-h-empty", "B4")
        h = "bench-h-empty"
        dead_prd = "PRD-B4DEAD"
        conn.execute(
            "INSERT INTO hierarchy_node (hierarchy_id, code, level, parent_code) "
            "VALUES (%s, %s, 'product', %s)",
            (h, dead_prd, fam),
        )
        dead_item = uuid4()
        conn.execute(
            "INSERT INTO items (item_id, name, item_type, uom, status, external_id) "
            "VALUES (%s, 'dead', 'finished_good', 'EA', 'active', 'BENCH-B4-DEAD')",
            (dead_item,),
        )
        conn.execute(
            "INSERT INTO item_hierarchy (item_id, hierarchy_id, leaf_code) "
            "VALUES (%s, %s, %s)",
            (dead_item, h, dead_prd),
        )
        # Demand only on a holdout day — nothing in [cutoff-lookback, cutoff).
        _insert_demand(conn, dead_item, "BENCH-B4-DEAD", CUTOFF + timedelta(days=3), 50)

        report = _run(conn)

        # The block was NOT aborted: the live siblings still produced rows.
        assert report.rows, "empty-history node aborted the whole block"
        leaf_mo = next(
            r for r in report.rows
            if r.level == LEVEL_LEAF and r.method == "middleout"
        )
        assert leaf_mo.n_series == 4  # 3 live leaves + the dead one
        assert leaf_mo.wape is not None
        assert report.verdicts()[fam] == "middleout"
        # ...and the dead subtree is reported, not silently swallowed.
        assert any(
            "forecasting zero for its subtree" in w for w in report.warnings
        ), report.warnings

    def test_grain_week_scores_four_complete_weeks(self, conn):
        """grain='week' on the SAME seed: FIXED_TODAY (2026-03-02) is a
        Monday, so it snaps to itself and cutoff = today - 4 weeks — the
        very same date as the daily CUTOFF. The 28 seeded holdout days
        are exactly 4 COMPLETE ISO weeks; the 84 train days are 12
        Monday-aligned weekly buckets, each summing the weekly pattern
        (amplitude x 14 — constant). Metrics must be finite where the
        accuracy contract defines them: wape/bias always (non-zero
        weekly actuals), mase None (weekly aggregation flattens the
        intra-week signal into a CONSTANT insample -> excluded, per the
        accuracy contract — not a bug, the contract working)."""
        from ootils_core.pyramide.hierarchy.bench import (
            GRAIN_WEEK,
            LEVEL_LEAF,
            LEVEL_ROOT,
            METHOD_BASE,
            run_reconciliation_bench,
        )

        fam, _ = _seed_block(conn, "bench-h-week", "B5")
        weeks = 4
        report = run_reconciliation_bench(
            conn,
            domain="product",
            recon_level="product",
            lookback_days=17,       # 17 WEEKS — covers the 12 seeded ones
            horizon=weeks,          # 4 WEEKLY buckets
            holdout_days=weeks,
            methods=("middleout",),
            today=FIXED_TODAY,
            grain=GRAIN_WEEK,
        )

        assert report.grain == GRAIN_WEEK
        assert report.cutoff == CUTOFF          # Monday-aligned snap
        assert report.cutoff.weekday() == 0     # ISO week start

        by_cell = {(r.level, r.method): r for r in report.rows}
        assert set(by_cell) == {
            (level, method)
            for level in (LEVEL_LEAF, "product", LEVEL_ROOT)
            for method in ("middleout", METHOD_BASE)
        }
        for level, n_series in [(LEVEL_LEAF, 3), ("product", 2), (LEVEL_ROOT, 1)]:
            row = by_cell[(level, "middleout")]
            assert row.block == fam
            assert row.n_series == n_series
            # 4 complete weekly buckets per series — nothing partial.
            assert row.n_obs == n_series * weeks
            assert row.wape is not None and row.wape >= 0, level
            assert row.bias is not None, level
            assert row.mase is None, level  # constant weekly insample
        assert report.verdicts()[fam] is not None

    def test_two_calls_same_report_and_verdict(self, conn):
        """Determinism: two identical calls (fixed ``today``) return equal
        BenchReports and the same non-None verdict."""
        fam, _ = _seed_block(conn, "bench-h-det", "B2")
        first = _run(conn)
        second = _run(conn)
        # frozen dataclasses of Decimals: byte-identical report
        assert first == second
        verdict = first.verdicts()
        assert verdict == second.verdicts()
        assert verdict[fam] is not None  # rankable, never arbitrary

    def test_holdout_insert_changes_metrics_not_forecasts(self, conn):
        """The anti-leak test: a demand line inserted INSIDE the holdout
        window (on the cutoff day itself — boundary belongs to EVAL, train
        is strictly before) must leave every training history — hence
        every forecast — unchanged, while the actuals-side metrics move."""
        fam, items = _seed_block(conn, "bench-h-leak", "B3")

        engine_before = _RecordingEngine()
        before = _run(conn, engine_before)

        # Big spike on the FIRST holdout day for leaf A1.
        _insert_demand(conn, items["A1"], "BENCH-B3-A1", CUTOFF, 500)

        engine_after = _RecordingEngine()
        after = _run(conn, engine_after)

        # 1. TRAIN saw nothing: the real engine received byte-identical
        #    histories, so (deterministic engine, seed 0) forecasts are
        #    byte-identical too.
        assert engine_before.histories == engine_after.histories
        assert engine_before.histories  # the bench did call the engine

        # 2. EVAL saw the spike: leaf-level metrics moved.
        def leaf_row(report, method):
            return next(
                r for r in report.rows
                if r.level == "leaf" and r.method == method
            )

        for method in ("middleout", "base"):
            b, a = leaf_row(before, method), leaf_row(after, method)
            assert b.wape != a.wape, method   # actuals changed under fixed forecasts
            assert b.bias != a.bias, method
            assert b.n_obs == a.n_obs, method  # same dense eval window
