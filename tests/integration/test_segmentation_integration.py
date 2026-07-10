"""
Integration tests for the buy-program segmentation DB half (DEM-2 PR1,
issue #444) against a real PostgreSQL database (no mocks).

Covered surfaces:
  1. src/ootils_core/pyramide/segmentation.py:get_historical_demand_by_program
     — the dense, per-program, alias-aware reader over ``demand_history``
     (migration 048's ``order_type``): shared zero-filled calendar, golden
     Σ(programs)=total, shared business predicates, cross-reader sum
     consistency with the leaf reader, org_id pooling (out of PR1 scope by
     contract), fail-loudly daily rejection with a live connection.
  2. ADR-031 alias resolution: two warehouse codes (external_id + alias in
     ``location_aliases``, migration 070) fold into ONE per-site calendar;
     another site's alias never leaks.
  3. scripts/prove_segmentation_fva.py — the proof harness run IN-PROCESS
     (both ``run()`` on a live connection and ``main()`` end-to-end through
     its own DSN connection) on a controlled seed: eligibility gate
     (>= 2 distinct non-NULL order_type values), computable ΔFVA with
     ``basis_count`` > 0, volume, aggregate line. READ-ONLY by construction:
     the harness writes nothing, so no harness-side cleanup exists.

Conventions (mirror test_demand_history_readers_integration.py /
test_location_aliases_integration.py): module-scoped ``migrated_db`` — the
seed script is deliberately NOT run, so the harness's eligible-series scan
sees ONLY the rows each test creates; autocommit ``_db_conn`` for
setup/teardown; every test creates its OWN item/location pair (fresh,
run-unique external_ids) and cleans up in a ``finally`` block, FK order
respected (location_aliases before locations — ON DELETE RESTRICT); the
baseline scenario is never touched (demand_history is scenario-invariant,
nothing here is scenario-scoped).

Seed contract: ``_insert_dh`` reproduces the demand_history column set the
sibling tests use (migration 047/048 reality: stream CHECK regular|warranty,
fulfillment CHECK standard|direct|inter_entity, item_code NOT NULL,
counts_for_asp NOT NULL, ingested_at NOT NULL default) with ``order_type``
and ``org_id`` (both nullable, 048) driven by each test.
"""
from __future__ import annotations

import sys
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from .conftest import requires_db, TEST_DB_URL

pytestmark = requires_db

REPO_ROOT = Path(__file__).resolve().parents[2]
TODAY = date.today()

# Unique per-run prefix so repeated runs against the same DB never collide on
# external_id (locations.external_id / items.external_id are UNIQUE).
PREFIX = str(uuid4())[:8]


def uid(base: str) -> str:
    return f"{PREFIX}-{base}"


# ---------------------------------------------------------------------------
# Import seam: the proof harness lives under scripts/ (outside the package) —
# same seam as test_demand_foundation_integration.py's ingest import.
# ---------------------------------------------------------------------------
_SCRIPTS_DIR = REPO_ROOT / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import prove_segmentation_fva as harness  # noqa: E402  (after sys.path mutation)

from ootils_core.pyramide.repository import get_historical_demand  # noqa: E402
from ootils_core.pyramide.segmentation import (  # noqa: E402
    BUCKET_BASE,
    BUCKET_SPRING,
    BUCKET_UNKNOWN,
    aggregate_delta_fva_wape,
    get_historical_demand_by_program,
    verify_partition_exhaustive,
)

D = Decimal


# ---------------------------------------------------------------------------
# Helpers — direct DB access (autocommit) for setup/teardown, bucket math
# ---------------------------------------------------------------------------


def _db_conn(dsn):
    import psycopg
    from psycopg.rows import dict_row
    return psycopg.connect(dsn, row_factory=dict_row, autocommit=True)


def _create_item(conn, item_ext: str) -> UUID:
    item_id = uuid4()
    conn.execute(
        """
        INSERT INTO items (item_id, name, item_type, uom, status, external_id)
        VALUES (%s, %s, 'finished_good', 'EA', 'active', %s)
        """,
        (item_id, f"{item_ext} test item", item_ext),
    )
    return item_id


def _create_location(conn, loc_ext: str) -> UUID:
    loc_id = uuid4()
    conn.execute(
        """
        INSERT INTO locations (location_id, name, location_type, country, external_id)
        VALUES (%s, %s, 'dc', 'US', %s)
        """,
        (loc_id, f"{loc_ext} test DC", loc_ext),
    )
    return loc_id


def _add_alias(conn, location_id: UUID, alias: str, source_system: str = "erp") -> None:
    conn.execute(
        """
        INSERT INTO location_aliases (location_id, alias, source_system)
        VALUES (%s, %s, %s)
        """,
        (location_id, alias, source_system),
    )


def _insert_dh(
    conn,
    item_id: UUID,
    item_code: str,
    warehouse_id: str | None,
    booked_date: date,
    qty,
    *,
    order_type: str | None = None,
    org_id: str | None = "PPS",
    stream: str = "regular",
    fulfillment: str | None = "standard",
):
    conn.execute(
        """
        INSERT INTO demand_history (
            item_id, item_code, stream, booked_date,
            ordered_quantity, value_ext, counts_for_asp,
            warehouse_id, fulfillment, order_number,
            order_type, org_id, ingested_at
        ) VALUES (%s, %s, %s, %s, %s, 0, FALSE, %s, %s, 'TEST-SEG', %s, %s, now())
        """,
        (item_id, item_code, stream, booked_date, qty, warehouse_id,
         fulfillment, order_type, org_id),
    )


def _cleanup_item(conn, item_id: UUID):
    conn.execute("DELETE FROM demand_history WHERE item_id = %s", (item_id,))
    conn.execute("DELETE FROM items WHERE item_id = %s", (item_id,))


def _cleanup_location(conn, location_id: UUID):
    """Aliases must go before the location (FK ON DELETE RESTRICT)."""
    conn.execute("DELETE FROM location_aliases WHERE location_id = %s", (location_id,))
    conn.execute("DELETE FROM locations WHERE location_id = %s", (location_id,))


def _week_start(d: date) -> date:
    """Monday of d's ISO week — the exact date_trunc('week') convention."""
    return d - timedelta(days=d.weekday())


def _month_start(d: date) -> date:
    return d.replace(day=1)


def _shift_months(d: date, months_back: int) -> date:
    """First of the month ``months_back`` months before d's month."""
    total = d.year * 12 + (d.month - 1) - months_back
    return date(total // 12, total % 12 + 1, 1)


def _reader(conn, item_id, location_id, lookback_days=90, granularity="weekly"):
    return get_historical_demand_by_program(
        conn, item_id, location_id, lookback_days, granularity
    )


# ===========================================================================
# 1. Dense per-program calendar from real demand_history rows
# ===========================================================================


class TestDenseCalendarReader:
    def test_two_programs_plus_null_share_a_zero_filled_calendar(self, migrated_db):
        """2 real programs + NULL order_type over 3 weeks with an EMPTY
        middle week: one shared dense calendar, per-program zero-fill,
        Σ(programs) == total, and the total is consistent with the leaf
        reader (same predicates, same alias resolution) summed over the
        window."""
        with _db_conn(migrated_db) as conn:
            item_id = _create_item(conn, uid("CAL-ITEM"))
            loc_id = _create_location(conn, uid("CAL-LOC"))
            w0 = _week_start(TODAY - timedelta(days=35))
            # Week 0: SPRING 10 + UNKNOWN (NULL order_type) 4.
            _insert_dh(conn, item_id, uid("CAL-ITEM"), uid("CAL-LOC"),
                       w0, 10, order_type="2026 SPRING BUY")
            _insert_dh(conn, item_id, uid("CAL-ITEM"), uid("CAL-LOC"),
                       w0 + timedelta(days=2), 4, order_type=None)
            # Week 1: nothing (the hole).
            # Week 2: BASE 6 + SPRING 2 (two rows, same program+week, summed).
            _insert_dh(conn, item_id, uid("CAL-ITEM"), uid("CAL-LOC"),
                       w0 + timedelta(days=14), 6, order_type="STANDARD VISTA")
            _insert_dh(conn, item_id, uid("CAL-ITEM"), uid("CAL-LOC"),
                       w0 + timedelta(days=15), 2, order_type="2026 SPRING BUY")
            try:
                calendar = _reader(conn, item_id, loc_id)

                assert calendar.granularity == "weekly"
                assert calendar.bucket_starts == (
                    w0, w0 + timedelta(days=7), w0 + timedelta(days=14),
                )
                assert calendar.programs == (
                    BUCKET_SPRING, BUCKET_BASE, BUCKET_UNKNOWN,
                )
                assert calendar.series_by_program[BUCKET_SPRING] == (
                    D(10), D(0), D(2),
                )
                assert calendar.series_by_program[BUCKET_BASE] == (
                    D(0), D(0), D(6),
                )
                assert calendar.series_by_program[BUCKET_UNKNOWN] == (
                    D(4), D(0), D(0),
                )
                assert calendar.total == (D(14), D(0), D(8))
                assert verify_partition_exhaustive(calendar)

                # Cross-reader consistency: same predicates + same alias
                # resolution => the bucketed total sums to the leaf reader's
                # daily series over the same lookback window.
                daily = get_historical_demand(
                    db=conn, item_id=item_id, location_id=loc_id,
                    lookback_days=90,
                )
                assert sum(calendar.total, D(0)) == sum(daily, D(0)) == D(22)
            finally:
                _cleanup_item(conn, item_id)
                _cleanup_location(conn, loc_id)

    def test_shared_business_predicates_filter_the_program_series(self, migrated_db):
        """The reader consumes the SAME shared predicates as the leaf reader:
        warranty stream, inter_entity fulfillment, today (partial day),
        beyond-lookback rows and NULL/unmatched warehouse rows all stay out —
        even when they carry a program order_type."""
        with _db_conn(migrated_db) as conn:
            item_id = _create_item(conn, uid("PRED-ITEM"))
            loc_id = _create_location(conn, uid("PRED-LOC"))
            keep_day = TODAY - timedelta(days=10)
            # Counted: regular / standard / strict past / within lookback.
            _insert_dh(conn, item_id, uid("PRED-ITEM"), uid("PRED-LOC"),
                       keep_day, 5, order_type="SPRING BUY")
            # Excluded: warranty stream.
            _insert_dh(conn, item_id, uid("PRED-ITEM"), uid("PRED-LOC"),
                       keep_day, 100, order_type="SPRING BUY", stream="warranty")
            # Excluded: inter-entity flow.
            _insert_dh(conn, item_id, uid("PRED-ITEM"), uid("PRED-LOC"),
                       keep_day, 100, order_type="SPRING BUY",
                       fulfillment="inter_entity")
            # Excluded: today (strict past).
            _insert_dh(conn, item_id, uid("PRED-ITEM"), uid("PRED-LOC"),
                       TODAY, 100, order_type="SPRING BUY")
            # Excluded: older than the lookback window.
            _insert_dh(conn, item_id, uid("PRED-ITEM"), uid("PRED-LOC"),
                       TODAY - timedelta(days=200), 100, order_type="SPRING BUY")
            # Excluded: NULL warehouse / unknown DC (per-site series).
            _insert_dh(conn, item_id, uid("PRED-ITEM"), None,
                       keep_day, 100, order_type="SPRING BUY")
            _insert_dh(conn, item_id, uid("PRED-ITEM"), uid("PRED-ORPHAN"),
                       keep_day, 100, order_type="SPRING BUY")
            try:
                calendar = _reader(conn, item_id, loc_id, lookback_days=90)
                assert calendar.bucket_starts == (_week_start(keep_day),)
                assert calendar.programs == (BUCKET_SPRING,)
                assert calendar.total == (D(5),)
            finally:
                _cleanup_item(conn, item_id)
                _cleanup_location(conn, loc_id)

    def test_org_id_is_pooled_without_filter(self, migrated_db):
        """org_id is OUT OF SCOPE for PR1 (extensible key, per contract):
        rows from different operating companies pool into the same series."""
        with _db_conn(migrated_db) as conn:
            item_id = _create_item(conn, uid("ORG-ITEM"))
            loc_id = _create_location(conn, uid("ORG-LOC"))
            day = TODAY - timedelta(days=8)
            _insert_dh(conn, item_id, uid("ORG-ITEM"), uid("ORG-LOC"),
                       day, 5, order_type=None, org_id="PPS")
            _insert_dh(conn, item_id, uid("ORG-ITEM"), uid("ORG-LOC"),
                       day, 7, order_type=None, org_id="PCC")
            _insert_dh(conn, item_id, uid("ORG-ITEM"), uid("ORG-LOC"),
                       day, 2, order_type=None, org_id=None)
            try:
                calendar = _reader(conn, item_id, loc_id)
                assert calendar.bucket_starts == (_week_start(day),)
                assert calendar.series_by_program[BUCKET_UNKNOWN] == (D(14),)
                assert calendar.total == (D(14),)
            finally:
                _cleanup_item(conn, item_id)
                _cleanup_location(conn, loc_id)

    def test_monthly_granularity_buckets_by_month_start(self, migrated_db):
        """Monthly date_trunc: rows land on first-of-month buckets and the
        calendar is dense between the first and last active month."""
        with _db_conn(migrated_db) as conn:
            item_id = _create_item(conn, uid("MON-ITEM"))
            loc_id = _create_location(conn, uid("MON-LOC"))
            d_old = TODAY - timedelta(days=100)
            d_new = TODAY - timedelta(days=40)
            _insert_dh(conn, item_id, uid("MON-ITEM"), uid("MON-LOC"),
                       d_old, 9, order_type="LESLIES FWD BUY")
            _insert_dh(conn, item_id, uid("MON-ITEM"), uid("MON-LOC"),
                       d_new, 3, order_type=None)
            try:
                calendar = _reader(
                    conn, item_id, loc_id, lookback_days=365,
                    granularity="monthly",
                )
                # Dense month range recomputed independently in the test.
                expected = []
                cursor = _month_start(d_old)
                while cursor <= _month_start(d_new):
                    expected.append(cursor)
                    cursor = _shift_months(cursor, -1)
                assert calendar.bucket_starts == tuple(expected)
                assert calendar.total[0] == D(9)
                assert calendar.total[-1] == D(3)
                assert sum(calendar.total, D(0)) == D(12)
                assert verify_partition_exhaustive(calendar)
            finally:
                _cleanup_item(conn, item_id)
                _cleanup_location(conn, loc_id)

    def test_no_rows_yield_the_empty_calendar(self, migrated_db):
        with _db_conn(migrated_db) as conn:
            item_id = _create_item(conn, uid("EMPTY-ITEM"))
            loc_id = _create_location(conn, uid("EMPTY-LOC"))
            try:
                calendar = _reader(conn, item_id, loc_id)
                assert calendar.bucket_starts == ()
                assert calendar.total == ()
                assert calendar.programs == ()
            finally:
                _cleanup_item(conn, item_id)
                _cleanup_location(conn, loc_id)

    def test_daily_granularity_rejected_on_a_live_connection(self, migrated_db):
        """Fail-loudly with a REAL connection too (the unit test proves the
        validation happens before any SQL; this pins the live behaviour)."""
        with _db_conn(migrated_db) as conn:
            item_id = _create_item(conn, uid("DAILY-ITEM"))
            loc_id = _create_location(conn, uid("DAILY-LOC"))
            try:
                with pytest.raises(ValueError, match="daily is out of scope"):
                    _reader(conn, item_id, loc_id, granularity="daily")
            finally:
                _cleanup_item(conn, item_id)
                _cleanup_location(conn, loc_id)


# ===========================================================================
# 2. ADR-031 — alias-aware site resolution (2 codes -> 1 series)
# ===========================================================================


class TestAliasAwareReader:
    def test_two_codes_for_one_site_fold_into_one_series(self, migrated_db):
        """demand_history rows split between the canonical external_id and a
        location_aliases code collapse into the SINGLE per-site calendar
        (same week summed, alias-only week present)."""
        with _db_conn(migrated_db) as conn:
            item_id = _create_item(conn, uid("AL-ITEM"))
            loc_id = _create_location(conn, uid("AL-LOC"))
            _add_alias(conn, loc_id, uid("AL-CODE-2"))
            w0 = _week_start(TODAY - timedelta(days=21))
            # Same week, one row per code: must SUM into one bucket.
            _insert_dh(conn, item_id, uid("AL-ITEM"), uid("AL-LOC"),
                       w0, 10, order_type="SPRING BUY")
            _insert_dh(conn, item_id, uid("AL-ITEM"), uid("AL-CODE-2"),
                       w0 + timedelta(days=1), 5, order_type="SPRING BUY")
            # A later week booked under the ALIAS code only.
            _insert_dh(conn, item_id, uid("AL-ITEM"), uid("AL-CODE-2"),
                       w0 + timedelta(days=7), 8, order_type=None)
            try:
                calendar = _reader(conn, item_id, loc_id)
                assert calendar.bucket_starts == (w0, w0 + timedelta(days=7))
                assert calendar.series_by_program[BUCKET_SPRING] == (D(15), D(0))
                assert calendar.series_by_program[BUCKET_UNKNOWN] == (D(0), D(8))
                assert calendar.total == (D(15), D(8))
                assert verify_partition_exhaustive(calendar)
            finally:
                _cleanup_item(conn, item_id)
                _cleanup_location(conn, loc_id)

    def test_other_sites_alias_does_not_leak(self, migrated_db):
        """Scoping is per location_id: demand booked under ANOTHER site's
        alias never enters this site's calendar (and does enter the owner's)."""
        with _db_conn(migrated_db) as conn:
            item_id = _create_item(conn, uid("LEAK-ITEM"))
            loc_a = _create_location(conn, uid("LEAK-LOC-A"))
            loc_b = _create_location(conn, uid("LEAK-LOC-B"))
            _add_alias(conn, loc_b, uid("LEAK-ALIAS-B"))
            day = TODAY - timedelta(days=9)
            _insert_dh(conn, item_id, uid("LEAK-ITEM"), uid("LEAK-LOC-A"),
                       day, 9, order_type="SPRING BUY")
            _insert_dh(conn, item_id, uid("LEAK-ITEM"), uid("LEAK-ALIAS-B"),
                       day, 100, order_type="SPRING BUY")
            try:
                cal_a = _reader(conn, item_id, loc_a)
                assert cal_a.total == (D(9),)
                cal_b = _reader(conn, item_id, loc_b)
                assert cal_b.total == (D(100),)
            finally:
                _cleanup_item(conn, item_id)
                _cleanup_location(conn, loc_a)
                _cleanup_location(conn, loc_b)


# ===========================================================================
# 3. Proof harness in-process — controlled seed, computable ΔFVA
# ===========================================================================


def _seed_eligible_series(conn, item_ext: str, loc_ext: str) -> tuple[UUID, UUID, Decimal]:
    """15 consecutive past months of demand with TWO distinct non-NULL
    order_type values (SPRING BUY + STANDARD VISTA) plus NULL rows (UNKNOWN
    bucket). 15 monthly buckets and --tail-origins 3 give min_train = 12 =
    the monthly season, the exact threshold where the seasonal-naive (and
    hence the FVA delta) becomes computable. Returns (item, location,
    expected volume)."""
    item_id = _create_item(conn, item_ext)
    loc_id = _create_location(conn, loc_ext)
    volume = D(0)
    for k in range(15, 0, -1):
        booked = _shift_months(_month_start(TODAY), k) + timedelta(days=4)
        spring = 20 + (k % 3)
        base = 10 + (k % 2)
        _insert_dh(conn, item_id, item_ext, loc_ext, booked, spring,
                   order_type="2026 SPRING BUY")
        _insert_dh(conn, item_id, item_ext, loc_ext, booked, base,
                   order_type="STANDARD VISTA")
        _insert_dh(conn, item_id, item_ext, loc_ext, booked, 3,
                   order_type=None)
        volume += D(spring) + D(base) + D(3)
    return item_id, loc_id, volume


class TestProofHarnessInProcess:
    def test_run_and_main_compute_delta_on_controlled_seed(
        self, migrated_db, capsys
    ):
        """End-to-end on a controlled seed: the harness discovers exactly the
        eligible series (>= 2 distinct non-NULL order_type values — the
        single-program sibling and the NULL-only signal do NOT qualify),
        builds the dense calendar, runs the AVANT/APRÈS backtest pair with
        the real stat engine and produces a COMPUTABLE ΔFVA (basis > 0),
        then main() renders the table + volume-weighted aggregate."""
        with _db_conn(migrated_db) as conn:
            item_id, loc_id, volume = _seed_eligible_series(
                conn, uid("HARN-ITEM"), uid("HARN-LOC")
            )
            # Ineligible sibling: ONE distinct order_type + NULL rows.
            # COUNT(DISTINCT order_type) ignores NULLs, so this series stays
            # below min_order_types=2 — order_type NULL is the ABSENCE of a
            # classification signal, not a program to prove segmentation on.
            other_item = _create_item(conn, uid("MONO-ITEM"))
            other_loc = _create_location(conn, uid("MONO-LOC"))
            for k in range(15, 0, -1):
                booked = _shift_months(_month_start(TODAY), k) + timedelta(days=4)
                _insert_dh(conn, other_item, uid("MONO-ITEM"), uid("MONO-LOC"),
                           booked, 50, order_type="STANDARD VISTA")
                _insert_dh(conn, other_item, uid("MONO-ITEM"), uid("MONO-LOC"),
                           booked, 5, order_type=None)
            try:
                rows = harness.run(
                    conn,
                    granularity="monthly",
                    lookback_days=600,
                    tail_origins=3,
                    horizon=1,
                    min_order_types=2,
                    top=5,
                )

                # Exactly the eligible series — the mono-program sibling is
                # filtered out by the >= 2 distinct order_type gate.
                assert [row.item_id for row in rows] == [item_id]
                row = rows[0]
                assert row.location_id == loc_id
                assert row.volume == volume

                result = row.result
                assert result.granularity == "monthly"
                assert result.n_buckets == 15
                assert result.min_train == 12
                assert result.programs == (
                    BUCKET_SPRING, BUCKET_BASE, BUCKET_UNKNOWN,
                )
                # The proof: a real, computable delta on 3 shared cutoffs
                # (15 buckets - min_train 12), never an invented one. The
                # SIGN is the stat engine's verdict on this seed — not
                # asserted; the machine, not the test, judges.
                assert result.basis_count == 3
                assert result.delta_fva_wape is not None
                assert result.fva_mixed is not None
                assert result.fva_mixed.naive_wape is not None
                assert result.delta_fva_wape == (
                    result.fva_segmented.fva_wape - result.fva_mixed.fva_wape
                )

                # Aggregate: one contributing series.
                weighted_mean, n_series = aggregate_delta_fva_wape(rows)
                assert weighted_mean == result.delta_fva_wape
                assert n_series == 1

                # main() end-to-end (own connection, read-only transaction
                # guard, argparse) on the same seed.
                rc = harness.main([
                    "--dsn", TEST_DB_URL,
                    "--granularity", "monthly",
                    "--lookback-days", "600",
                    "--tail-origins", "3",
                    "--top", "5",
                ])
                assert rc == 0
                out = capsys.readouterr().out
                assert uid("HARN-ITEM") in out
                assert uid("MONO-ITEM") not in out
                assert "AGGREGATE dFVA_wape (volume-weighted):" in out
            finally:
                _cleanup_item(conn, item_id)
                _cleanup_item(conn, other_item)
                _cleanup_location(conn, loc_id)
                _cleanup_location(conn, other_loc)

    def test_harness_discovery_is_alias_aware(self, migrated_db, capsys):
        """Every demand row of the eligible series is booked under an ALIAS
        code (never the canonical external_id): the discovery SQL's
        site_codes UNION must still resolve the series to its owning
        location, and the reader must fold it (ADR-031 in the harness's
        reverse direction)."""
        with _db_conn(migrated_db) as conn:
            item_id = _create_item(conn, uid("ALH-ITEM"))
            loc_id = _create_location(conn, uid("ALH-LOC"))
            _add_alias(conn, loc_id, uid("ALH-87"))
            for k in range(15, 0, -1):
                booked = _shift_months(_month_start(TODAY), k) + timedelta(days=4)
                _insert_dh(conn, item_id, uid("ALH-ITEM"), uid("ALH-87"),
                           booked, 12 + (k % 4), order_type="CN EARLY BUY")
                _insert_dh(conn, item_id, uid("ALH-ITEM"), uid("ALH-87"),
                           booked, 6, order_type="STANDARD VISTA")
            try:
                rows = harness.run(
                    conn,
                    granularity="monthly",
                    lookback_days=600,
                    tail_origins=3,
                    horizon=1,
                    min_order_types=2,
                    top=5,
                )
                assert [row.item_id for row in rows] == [item_id]
                assert rows[0].location_id == loc_id  # canonical site, not the code
                assert rows[0].result.basis_count == 3
                assert rows[0].result.delta_fva_wape is not None
            finally:
                _cleanup_item(conn, item_id)
                _cleanup_location(conn, loc_id)
