"""
Integration tests for the demand foundation (#344) against a real
PostgreSQL database (no mocks).

Covered layer (migrations 047-050 + operational scripts):
  - demand_history           (047, 048) — sign rule, streams, counts_for_asp
  - item_asp                 (049)      — golden-master compute_asp.py
  - returns_history          (050)      — separate series, never netted
  - scripts/ingest_demand_history.py    — classification + sign rule +
                                          delete-window idempotence (real
                                          subprocess --load contract)
  - scripts/ingest_returns.py           — positive magnitudes contract
  - scripts/compute_asp.py              — T12M window, exclusions, per-org

Conventions (mirrors test_demand_history_readers_integration.py):
  - module-scoped migrated_db, autocommit _db_conn for seeding/teardown,
  - every test creates its OWN items (fresh external_ids) and cleans up in
    a finally block — the shared seed and sibling tests are never perturbed.

Import seam: the ingestion helpers live under scripts/ (outside the
package); iter_demand_rows/load_classification are imported directly so
the tests exercise the SAME code path `--load` streams through.

Direct-INSERT contract note: helpers that INSERT into demand_history /
returns_history reproduce the exact column list the scripts COPY
(ingest_demand_history.DH_COLS / ingest_returns.COLS); rows the scripts
cannot produce (counts_for_asp=TRUE with a hand-picked value_ext) are used
only where the test needs a controlled golden dataset.
"""
from __future__ import annotations

import os
import subprocess
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from .conftest import requires_db, TEST_DB_URL

# NOTE: three tests in this module are pure (iter_demand_rows contract, no
# DB) and would normally live under tests/ per the project convention. They
# stay here because they share this module's script-import harness
# (scripts/ isn't a package) and TSV fixture builders; the requires_db mark
# only costs them a skip on DB-less local runs — CI's integration job (which
# always has Postgres) runs them on every push.
pytestmark = requires_db

REPO_ROOT = Path(__file__).resolve().parents[2]
INGEST_SCRIPT = REPO_ROOT / "scripts" / "ingest_demand_history.py"
RETURNS_SCRIPT = REPO_ROOT / "scripts" / "ingest_returns.py"
ASP_SCRIPT = REPO_ROOT / "scripts" / "compute_asp.py"
CLASSIFICATION_TSV = REPO_ROOT / "data" / "demand" / "line_type_classification.tsv"

# ---------------------------------------------------------------------------
# Import seam: the ingestion module lives under scripts/ (outside the package).
# ---------------------------------------------------------------------------
_SCRIPTS_DIR = REPO_ROOT / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import ingest_demand_history as ingest  # noqa: E402  (after sys.path mutation)


# ---------------------------------------------------------------------------
# Helpers — DB access, seeding, teardown
# ---------------------------------------------------------------------------


def _db_conn(dsn):
    import psycopg
    from psycopg.rows import dict_row
    return psycopg.connect(dsn, row_factory=dict_row, autocommit=True)


def _create_item(conn, ext_id: str) -> UUID:
    """Fresh item with a unique external_id (the ingestion resolution key)."""
    item_id = uuid4()
    conn.execute(
        """
        INSERT INTO items (item_id, name, item_type, uom, status, external_id)
        VALUES (%s, %s, 'finished_good', 'EA', 'active', %s)
        """,
        (item_id, f"{ext_id} test item", ext_id),
    )
    return item_id


def _insert_dh(
    conn,
    item_id: UUID | None,
    item_code: str,
    booked_date: date,
    qty,
    *,
    stream: str = "regular",
    value_ext=0,
    counts_for_asp: bool = False,
    org_id: str = "PPS",
    fulfillment: str | None = "standard",
):
    """INSERT one demand fact — same column contract as the script's COPY
    (ingest_demand_history.DH_COLS), values controlled by the test."""
    conn.execute(
        """
        INSERT INTO demand_history (
            item_id, item_code, stream, booked_date,
            ordered_quantity, value_ext, counts_for_asp,
            org_id, fulfillment, order_number
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'TEST-DF')
        """,
        (item_id, item_code, stream, booked_date, qty, value_ext,
         counts_for_asp, org_id, fulfillment),
    )


def _cleanup_item(conn, item_id: UUID, item_code: str):
    """Teardown of everything a test attached to its item (FK order:
    facts first, then the item — items FKs are ON DELETE RESTRICT)."""
    conn.execute("DELETE FROM item_asp WHERE item_id = %s", (item_id,))
    conn.execute(
        "DELETE FROM demand_history WHERE item_id = %s OR item_code = %s",
        (item_id, item_code),
    )
    conn.execute(
        "DELETE FROM returns_history WHERE item_id = %s OR item_code = %s",
        (item_id, item_code),
    )
    conn.execute("DELETE FROM items WHERE item_id = %s", (item_id,))


# ---------------------------------------------------------------------------
# Helpers — SALES_MARINE-shaped TSV fixtures + script subprocess driver
# ---------------------------------------------------------------------------

# The raw export is wide; the ingesters read by 0-based index (the highest
# is C_FULFILLED = 51), so fixture rows carry 52 columns.
_N_SALES_COLS = 52


def _sales_line(
    *,
    booked: str,
    item: str,
    qty,
    line_type: str,
    amount=0,
    ship_date: str = "",
    org: str = "PPS",
    country: str = "US",
    channel: str = "POOL",
    state: str = "FL",
    warehouse: str = "DC-TST",
    order_num: str = "SO-TST",
    order_type: str = "STANDARD VISTA",
    line_id: str = "",
    fulfilled="",
) -> str:
    """One SALES_MARINE-shaped row, using the exact column indices the
    ingestion scripts read (C_SHIP_DATE=0, C_BOOKED=1, C_ITEM=3, ...)."""
    r = [""] * _N_SALES_COLS
    r[ingest.C_SHIP_DATE] = ship_date
    r[ingest.C_BOOKED] = booked
    r[ingest.C_ITEM] = item
    r[ingest.C_AMOUNT] = str(amount)
    r[ingest.C_LINE_ID] = line_id
    r[ingest.C_LINE_TYPE] = line_type
    r[ingest.C_ORDER_NUM] = order_num
    r[ingest.C_ORDER_TYPE] = order_type
    r[ingest.C_QTY] = str(qty)
    r[ingest.C_ORG] = org
    r[ingest.C_COUNTRY] = country
    r[ingest.C_CHANNEL] = channel
    r[ingest.C_STATE] = state
    r[ingest.C_WAREHOUSE] = warehouse
    r[ingest.C_FULFILLED] = str(fulfilled)
    return "\t".join(r)


def _write_sales_tsv(path: Path, lines: list[str]) -> Path:
    header = "\t".join(f"col{i}" for i in range(_N_SALES_COLS))
    path.write_text(header + "\n" + "\n".join(lines) + "\n", encoding="utf-8")
    return path


def _run_script(script: Path, *args: str) -> subprocess.CompletedProcess:
    env = {**os.environ, "DATABASE_URL": TEST_DB_URL}
    return subprocess.run(
        [sys.executable, str(script), *args],
        capture_output=True, text=True, env=env, cwd=str(REPO_ROOT),
    )


def _classification() -> dict:
    return ingest.load_classification(CLASSIFICATION_TSV)


# ---------------------------------------------------------------------------
# (1) Sign rule — ingestion only ever loads positive magnitudes (rule 047)
# ---------------------------------------------------------------------------


class TestSignRule:
    def test_iter_demand_rows_yields_positive_quantities_only(self, tmp_path):
        """iter_demand_rows (the exact generator --load COPYs from) drops
        negative and zero quantities, and non-demand LINE_TYPEs, using the
        REAL classification table."""
        tsv = _write_sales_tsv(tmp_path / "sales_sign.tsv", [
            _sales_line(booked="2099-06-10", item="DF-SIGN", qty=5,
                        line_type="STANDARD LINE", amount=100),
            # sign rule: negative = return, never demand
            _sales_line(booked="2099-06-11", item="DF-SIGN", qty=-3,
                        line_type="STANDARD LINE", amount=-60),
            # sign rule: zero is not demand either
            _sales_line(booked="2099-06-12", item="DF-SIGN", qty=0,
                        line_type="STANDARD LINE"),
            # negative on a RETURN class (in_demand=no): doubly excluded
            _sales_line(booked="2099-06-13", item="DF-SIGN", qty=-4,
                        line_type="CREDIT ONLY", amount=-80),
            # positive but in_demand=no class: excluded by classification
            _sales_line(booked="2099-06-14", item="DF-SIGN", qty=7,
                        line_type="NO BILL LINE"),
        ])
        rows = list(ingest.iter_demand_rows(tsv, _classification(), "2099-01-01"))
        assert len(rows) == 1
        assert rows[0]["ordered_quantity"] == 5
        assert all(r["ordered_quantity"] > 0 for r in rows)

    def test_loaded_rows_are_all_positive_in_db(self, migrated_db, tmp_path):
        """End-to-end --load: only the positive-magnitude line lands in
        demand_history (same fixture mix as above, real subprocess)."""
        with _db_conn(migrated_db) as conn:
            item_id = _create_item(conn, "DF-SIGN-DB")
        tsv = _write_sales_tsv(tmp_path / "sales_sign_db.tsv", [
            _sales_line(booked="2099-06-10", item="DF-SIGN-DB", qty=5,
                        line_type="STANDARD LINE", amount=100),
            _sales_line(booked="2099-06-11", item="DF-SIGN-DB", qty=-3,
                        line_type="STANDARD LINE", amount=-60),
        ])
        try:
            result = _run_script(
                INGEST_SCRIPT, "--load", "--since", "2099-01-01",
                "--sales", str(tsv), "--classification", str(CLASSIFICATION_TSV),
            )
            assert result.returncode == 0, result.stderr
            with _db_conn(migrated_db) as conn:
                rows = conn.execute(
                    "SELECT ordered_quantity, item_id FROM demand_history "
                    "WHERE item_code = 'DF-SIGN-DB'"
                ).fetchall()
            assert len(rows) == 1
            assert rows[0]["ordered_quantity"] == Decimal("5")
            assert rows[0]["item_id"] == item_id  # external_id resolution
        finally:
            with _db_conn(migrated_db) as conn:
                _cleanup_item(conn, item_id, "DF-SIGN-DB")


# ---------------------------------------------------------------------------
# (2) Streams — regular vs warranty are strictly separated
# ---------------------------------------------------------------------------


class TestStreamSeparation:
    def test_regular_query_never_sees_warranty_rows(self, migrated_db):
        with _db_conn(migrated_db) as conn:
            item_id = _create_item(conn, "DF-STREAM")
            _insert_dh(conn, item_id, "DF-STREAM", date(2025, 6, 1), 10)
            _insert_dh(conn, item_id, "DF-STREAM", date(2025, 6, 2), 4)
            _insert_dh(conn, item_id, "DF-STREAM", date(2025, 6, 3), 99,
                       stream="warranty")
            try:
                regular = conn.execute(
                    "SELECT ordered_quantity FROM demand_history "
                    "WHERE item_id = %s AND stream = 'regular' "
                    "ORDER BY booked_date",
                    (item_id,),
                ).fetchall()
                assert [r["ordered_quantity"] for r in regular] == [10, 4]
                warranty = conn.execute(
                    "SELECT ordered_quantity FROM demand_history "
                    "WHERE item_id = %s AND stream = 'warranty'",
                    (item_id,),
                ).fetchall()
                assert [r["ordered_quantity"] for r in warranty] == [99]
            finally:
                _cleanup_item(conn, item_id, "DF-STREAM")

    def test_stream_check_constraint_rejects_unknown_stream(self, migrated_db):
        """Migration 047 CHECK: stream IN ('regular','warranty') only."""
        import psycopg
        with _db_conn(migrated_db) as conn:
            item_id = _create_item(conn, "DF-STREAM-CK")
            try:
                with pytest.raises(psycopg.errors.CheckViolation):
                    _insert_dh(conn, item_id, "DF-STREAM-CK",
                               date(2025, 6, 1), 1, stream="returns")
            finally:
                _cleanup_item(conn, item_id, "DF-STREAM-CK")

    def test_warranty_line_classified_to_warranty_stream(self, tmp_path):
        """The classification maps WARRANTY CS to the warranty stream —
        warranty demand never contaminates the regular series."""
        tsv = _write_sales_tsv(tmp_path / "sales_stream.tsv", [
            _sales_line(booked="2099-06-10", item="DF-ST", qty=2,
                        line_type="STANDARD LINE", amount=40),
            _sales_line(booked="2099-06-10", item="DF-ST", qty=3,
                        line_type="WARRANTY CS", amount=0),
        ])
        rows = list(ingest.iter_demand_rows(tsv, _classification(), None))
        streams = sorted(r["stream"] for r in rows)
        assert streams == ["regular", "warranty"]


# ---------------------------------------------------------------------------
# (3) counts_for_asp — warranty / interco / dropship never count in ASP
# ---------------------------------------------------------------------------


class TestCountsForAsp:
    def test_asp_exclusions_flagged_and_value_zeroed(self, tmp_path):
        """Classification contract: in_asp=no (warranty, intercompany,
        drop-ship) → counts_for_asp=False AND value_ext forced to 0, so no
        downstream sum can ever pick them up. in_asp=yes keeps the value."""
        tsv = _write_sales_tsv(tmp_path / "sales_asp_flags.tsv", [
            _sales_line(booked="2099-06-10", item="DF-ASPF", qty=2,
                        line_type="STANDARD LINE", amount=50),
            _sales_line(booked="2099-06-10", item="DF-ASPF", qty=1,
                        line_type="WARRANTY CS", amount=30),
            _sales_line(booked="2099-06-10", item="DF-ASPF", qty=3,
                        line_type="INTERCOMPANY LINE", amount=90),
            _sales_line(booked="2099-06-10", item="DF-ASPF", qty=4,
                        line_type="CMP DROP SHIP LINE", amount=100),
        ])
        rows = list(ingest.iter_demand_rows(tsv, _classification(), None))
        assert len(rows) == 4  # all four classes are in_demand=yes
        eligible = [r for r in rows if r["counts_for_asp"]]
        excluded = [r for r in rows if not r["counts_for_asp"]]
        assert len(eligible) == 1 and eligible[0]["value_ext"] == 50.0
        assert len(excluded) == 3
        assert all(r["value_ext"] == 0.0 for r in excluded)
        # routing carried through: interco and dropship are tagged
        assert {r["fulfillment"] for r in excluded} == {
            "standard", "inter_entity", "direct"
        }


# ---------------------------------------------------------------------------
# (4) Ingestion idempotence — the delete-window makes --load re-runnable
# ---------------------------------------------------------------------------


class TestIngestionIdempotence:
    def test_double_load_does_not_duplicate(self, migrated_db, tmp_path):
        """Real contract test: run scripts/ingest_demand_history.py --load
        TWICE on the same fixture TSV; the script deletes the loaded window
        (booked_date >= --since) before COPYing, so row count and totals are
        identical after each run.

        The fixture uses a far-future window (2099) so the delete-window
        cannot touch rows other tests seeded near today.
        """
        with _db_conn(migrated_db) as conn:
            item_id = _create_item(conn, "DF-IDEM")
        tsv = _write_sales_tsv(tmp_path / "sales_idem.tsv", [
            _sales_line(booked="2099-06-10", item="DF-IDEM", qty=5,
                        line_type="STANDARD LINE", amount=100),
            _sales_line(booked="2099-06-11", item="DF-IDEM", qty=3,
                        line_type="STANDARD LINE", amount=60),
            _sales_line(booked="2099-06-12", item="DF-IDEM", qty=2,
                        line_type="WARRANTY CS", amount=0),
        ])
        args = ("--load", "--since", "2099-01-01",
                "--sales", str(tsv), "--classification", str(CLASSIFICATION_TSV))

        def _snapshot():
            with _db_conn(migrated_db) as conn:
                return conn.execute(
                    "SELECT COUNT(*) AS n, SUM(ordered_quantity) AS units, "
                    "SUM(value_ext) AS value FROM demand_history "
                    "WHERE item_code = 'DF-IDEM'"
                ).fetchone()

        try:
            first = _run_script(INGEST_SCRIPT, *args)
            assert first.returncode == 0, first.stderr
            snap1 = _snapshot()
            assert snap1["n"] == 3
            assert snap1["units"] == Decimal("10")
            assert snap1["value"] == Decimal("160")  # warranty value zeroed

            second = _run_script(INGEST_SCRIPT, *args)
            assert second.returncode == 0, second.stderr
            assert "cleared 3 existing rows" in second.stdout
            assert _snapshot() == snap1  # no duplication, same totals
        finally:
            with _db_conn(migrated_db) as conn:
                _cleanup_item(conn, item_id, "DF-IDEM")


# ---------------------------------------------------------------------------
# (5) Golden ASP — compute_asp.py on a controlled dataset
# ---------------------------------------------------------------------------


class TestGoldenAsp:
    def test_compute_asp_golden_values(self, migrated_db):
        """Golden master for scripts/compute_asp.py (hand-computed).

        Dataset (all rows org PPS unless noted, fixed dates so the T12M
        window is deterministic — window_end = max(booked_date)):

          GOLD-A / PPS, counts_for_asp=TRUE, in window:
              2025-06-15  qty 10   value 250
              2025-08-10  qty  5   value 125
          GOLD-A / PPS, counts_for_asp=FALSE (warranty): 2025-07-01 qty 100
          GOLD-A / PPS, counts_for_asp=TRUE but OUT of the T12M window
              (window_start = 2024-09-15): 2024-01-01 qty 1000 value 60000
          GOLD-A / PCC, counts_for_asp=TRUE: 2025-09-15 qty 4 value 130
          GOLD-B / PPS, counts_for_asp=FALSE only: 2025-08-01 qty 50

        Expected item_asp rows:
          (GOLD-A, PPS): units_12m = 10+5 = 15, value_12m = 250+125 = 375
                         asp = 375/15 = 25.000000
          (GOLD-A, PCC): units_12m = 4, value_12m = 130
                         asp = 130/4 = 32.500000   (per-org = per-currency)
          GOLD-B: NO row (nothing ASP-eligible)
          window = [2024-09-15, 2025-09-15]
        """
        with _db_conn(migrated_db) as conn:
            item_a = _create_item(conn, "DF-GOLD-A")
            item_b = _create_item(conn, "DF-GOLD-B")
            _insert_dh(conn, item_a, "DF-GOLD-A", date(2025, 6, 15), 10,
                       value_ext=250, counts_for_asp=True)
            _insert_dh(conn, item_a, "DF-GOLD-A", date(2025, 8, 10), 5,
                       value_ext=125, counts_for_asp=True)
            _insert_dh(conn, item_a, "DF-GOLD-A", date(2025, 7, 1), 100,
                       stream="warranty", value_ext=0, counts_for_asp=False)
            _insert_dh(conn, item_a, "DF-GOLD-A", date(2024, 1, 1), 1000,
                       value_ext=60000, counts_for_asp=True)
            _insert_dh(conn, item_a, "DF-GOLD-A", date(2025, 9, 15), 4,
                       value_ext=130, counts_for_asp=True, org_id="PCC")
            _insert_dh(conn, item_b, "DF-GOLD-B", date(2025, 8, 1), 50,
                       value_ext=0, counts_for_asp=False)
        try:
            result = _run_script(ASP_SCRIPT)
            assert result.returncode == 0, result.stderr

            with _db_conn(migrated_db) as conn:
                rows = conn.execute(
                    "SELECT org_id, asp, units_12m, value_12m, "
                    "window_start, window_end FROM item_asp "
                    "WHERE item_id = %s ORDER BY org_id",
                    (item_a,),
                ).fetchall()
                assert len(rows) == 2

                pcc, pps = rows  # ordered by org_id: PCC < PPS
                assert pps["org_id"] == "PPS"
                assert pps["asp"] == Decimal("25.000000")
                assert pps["units_12m"] == Decimal("15")
                assert pps["value_12m"] == Decimal("375")

                assert pcc["org_id"] == "PCC"
                assert pcc["asp"] == Decimal("32.500000")
                assert pcc["units_12m"] == Decimal("4")
                assert pcc["value_12m"] == Decimal("130")

                # deterministic trailing-12-month window
                for r in rows:
                    assert r["window_end"] == date(2025, 9, 15)
                    assert r["window_start"] == date(2024, 9, 15)

                # GOLD-B has no ASP-eligible demand: no row at all
                assert conn.execute(
                    "SELECT COUNT(*) AS n FROM item_asp WHERE item_id = %s",
                    (item_b,),
                ).fetchone()["n"] == 0
        finally:
            with _db_conn(migrated_db) as conn:
                _cleanup_item(conn, item_a, "DF-GOLD-A")
                _cleanup_item(conn, item_b, "DF-GOLD-B")


# ---------------------------------------------------------------------------
# (6) returns_history — separate series, positive magnitudes, never netted
# ---------------------------------------------------------------------------


class TestReturnsHistory:
    def test_returns_positive_magnitudes_and_demand_untouched(
        self, migrated_db, tmp_path
    ):
        """scripts/ingest_returns.py keeps only qty < 0 lines, stores their
        ABSOLUTE magnitudes in returns_history, and leaves demand_history
        strictly unchanged — the two series are independent (rule 050:
        returns are NEVER netted into demand)."""
        with _db_conn(migrated_db) as conn:
            item_id = _create_item(conn, "DF-RET")
            # pre-existing demand for the same item (must survive untouched)
            _insert_dh(conn, item_id, "DF-RET", date(2025, 5, 1), 15,
                       value_ext=300, counts_for_asp=True)

            def _demand_snapshot():
                return conn.execute(
                    "SELECT COUNT(*) AS n, SUM(ordered_quantity) AS units "
                    "FROM demand_history WHERE item_id = %s",
                    (item_id,),
                ).fetchone()

            before = _demand_snapshot()
            assert before == {"n": 1, "units": Decimal("15")}

        tsv = _write_sales_tsv(tmp_path / "sales_returns.tsv", [
            _sales_line(booked="2025-05-05", item="DF-RET", qty=-4,
                        line_type="CREDIT ONLY", amount=-100),
            _sales_line(booked="2025-05-06", item="DF-RET", qty=-2,
                        line_type="RECEIVE ONLY LINE", amount=0),
            # positive line: demand, must NOT land in returns_history
            _sales_line(booked="2025-05-07", item="DF-RET", qty=5,
                        line_type="STANDARD LINE", amount=100),
        ])
        try:
            result = _run_script(RETURNS_SCRIPT, "--sales", str(tsv))
            assert result.returncode == 0, result.stderr

            with _db_conn(migrated_db) as conn:
                returns = conn.execute(
                    "SELECT item_id, return_quantity, return_value, line_type "
                    "FROM returns_history WHERE item_code = 'DF-RET' "
                    "ORDER BY return_date",
                ).fetchall()
                # only the 2 negative lines, stored as POSITIVE magnitudes
                assert len(returns) == 2
                assert [r["return_quantity"] for r in returns] == [
                    Decimal("4"), Decimal("2"),
                ]
                assert [r["return_value"] for r in returns] == [
                    Decimal("100"), Decimal("0"),
                ]
                assert all(r["item_id"] == item_id for r in returns)
                assert {r["line_type"] for r in returns} == {
                    "CREDIT ONLY", "RECEIVE ONLY LINE",
                }

                # demand_history untouched: nothing netted, nothing added
                after = conn.execute(
                    "SELECT COUNT(*) AS n, SUM(ordered_quantity) AS units "
                    "FROM demand_history WHERE item_id = %s",
                    (item_id,),
                ).fetchone()
                assert after == {"n": 1, "units": Decimal("15")}
        finally:
            with _db_conn(migrated_db) as conn:
                _cleanup_item(conn, item_id, "DF-RET")
