"""
tests/integration/test_outbound_export_integration.py — the outbound-export
write path (engine/reporting/outbound_export.py, ADR-042 decision 4, PR-5)
against a real PostgreSQL — no DB mocks (CLAUDE.md). The pure TSV renderer
lives in tests/test_outbound_export.py.

Five axes:

  1. FULL STATUS x FAMILY MATRIX -> execute_export: recommendations seeded in
     EVERY status (DRAFT/REVIEWED/APPROVED/APPLIED/REJECTED/EXPIRED) for each
     outbound family (ORDER/RESCHEDULE/TRANSFER), plus an APPROVED row
     already stamped exported_at and an APPROVED row on a fork scenario.
     ONLY the baseline APPROVED+APPLIED rows with exported_at IS NULL are
     exported, each into its family file (règles d'or re-checked on the REAL
     bytes: UTF-8 no BOM, LF-only, exactly one trailing newline), exported_at
     is stamped for EXACTLY them (the pre-stamped row's timestamp untouched),
     and exactly ONE export_executed event carries the full typed-column
     contract (field_changed / new_date / new_quantity / new_text — emit.py's
     contract block; baseline scenario, source 'engine').

  2. IMMEDIATE RE-RUN — a second execute_export on the same connection: zero
     new file (the second outbox dir is never even created), zero re-stamp
     (every timestamp unchanged), zero new event. Idempotence by construction
     of the ``exported_at IS NULL`` scan.

  3. DRY-RUN — committed pending seed, execute_export(dry_run=True) on a
     connection that then COMMITS, and a FRESH connection proves zero writes:
     every exported_at still NULL, zero export_executed row, the outbox dir
     never created — while the RETURNED preview render is the real one.

  4. KILL SWITCH — the CLI phase (scripts/run_daily_ingest.py:
     _run_outbound_export): with OOTILS_OUTBOUND_EXPORT_ENABLED unset or '0'
     an --apply run previews to STDOUT and writes NOTHING (no file, no stamp,
     no event); with the switch ON the same call writes files + stamps + one
     event (the double-guard passes through); a global dry-run (apply=False)
     stays a preview even with the switch ON.

  5. MIGRATION 085 TRIPLE IDEMPOTENCE — re-executing the file verbatim twice
     on top of the migrated_db boot (defensive-idempotence contract,
     migration 063 header; the runner does NOT swallow 'already exists'):
     exactly one events_event_type_check survives, still accepting
     'export_executed' AND the pre-085 cumulative list, still rejecting an
     unknown type.

ISOLATION (pattern of test_daily_report_integration.py — unique PREFIX +
finalizer-DEACTIVATION; api_client is deliberately absent: PR-5 exposes no
HTTP surface, the export runs through the engine call and the CLI phase, so
there is no endpoint for a TestClient to exercise): axes 1/2/4/5 commit
NOTHING — every seed and every execute_export rides the rollback-teardown
``conn`` fixture (execute_export never commits by contract, so the rollback
sweeps events + stamps away). Axis 3 must commit its seed (a fresh
connection has to see it), so its finalizer — registered BEFORE the commit,
the migration-083-test lesson — neutralizes by DEACTIVATION: the pending
rows get exported_at stamped (invisible to every later pending-export scan),
never a DELETE (recommendations are governed audit rows, ADR-039's exempt
family). The autouse module sweep pre-cleans committed 'export_executed'
leftovers (surgical events DELETE, convention of
test_daily_run_decision_integration.py) and neutralizes any foreign
committed pending-export row the same stamp-only way.
"""
from __future__ import annotations

import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg import errors
from psycopg.rows import dict_row

from ootils_core.constants import BASELINE_SCENARIO_ID
from ootils_core.engine.reporting.outbound_export import execute_export

from .conftest import requires_db

# Import seam: run_daily_ingest lives under scripts/ (outside the package),
# exactly as the sibling watcher/orchestrator integration tests.
_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCRIPTS_DIR = _REPO_ROOT / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import run_daily_ingest  # noqa: E402

pytestmark = requires_db

PREFIX = f"OBX-{uuid4().hex[:8]}"
AGENT = f"outbound-export-itest-{PREFIX}"

ALL_STATUSES = ("DRAFT", "REVIEWED", "APPROVED", "APPLIED", "REJECTED", "EXPIRED")

MIGRATION_085 = (
    _REPO_ROOT
    / "src"
    / "ootils_core"
    / "db"
    / "migrations"
    / "085_export_executed_event.sql"
)

NOW_1 = datetime(2026, 7, 18, 6, 30, tzinfo=timezone.utc)
PAST_TS = datetime(2026, 7, 1, 5, 0, tzinfo=timezone.utc)

PO_HEADER = [
    "item_external_id", "supplier_external_id", "quantity", "need_date",
    "action", "recommendation_id", "confidence",
]
RESCHEDULE_HEADER = [
    "item_external_id", "target_po_reference", "current_receipt_date",
    "proposed_date", "action", "recommendation_id",
]
TRANSFERS_HEADER = [
    "item_external_id", "source_location_external_id",
    "dest_location_external_id", "quantity", "shortage_date",
    "recommendation_id",
]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module", autouse=True)
def _module_sweep(migrated_db):
    """Belt-and-braces pre-clean of committed leftovers a crashed prior
    module could have left behind: surgical DELETE of export_executed events
    (the convention of test_daily_run_decision_integration.py's sweep) +
    stamp-only DEACTIVATION of any foreign committed pending-export row —
    never a DELETE of recommendations (governed audit rows, ADR-039)."""
    with psycopg.connect(migrated_db, autocommit=True) as c:
        c.execute("DELETE FROM events WHERE event_type = 'export_executed'")
        c.execute(
            "UPDATE recommendations SET exported_at = now() "
            "WHERE scenario_id = %s AND status IN ('APPROVED', 'APPLIED') "
            "  AND exported_at IS NULL",
            (BASELINE_SCENARIO_ID,),
        )
    yield


# ---------------------------------------------------------------------------
# Seed helpers — all write on the CALLER's connection, never commit.
# ---------------------------------------------------------------------------


def _seed_agent_run(conn) -> UUID:
    run_id = uuid4()
    conn.execute(
        "INSERT INTO agent_runs (agent_run_id, agent_name, scenario_id, status) "
        "VALUES (%s, %s, %s, 'COMPLETED')",
        (run_id, AGENT, BASELINE_SCENARIO_ID),
    )
    return run_id


def _seed_location(conn, code: str) -> UUID:
    loc_id = uuid4()
    conn.execute(
        "INSERT INTO locations (location_id, external_id, name) VALUES (%s, %s, %s)",
        (loc_id, code, code),
    )
    return loc_id


def _seed_node(conn) -> UUID:
    """A minimal baseline receipt node — the reschedule target coordinate."""
    node_id = uuid4()
    conn.execute(
        "INSERT INTO nodes (node_id, node_type, scenario_id, time_grain, time_ref, active) "
        "VALUES (%s, 'PurchaseOrderSupply', %s, 'exact_date', %s, TRUE)",
        (node_id, BASELINE_SCENARIO_ID, date(2026, 9, 1)),
    )
    return node_id


def _seed_po_ref(conn, node_id: UUID, external_id: str) -> None:
    conn.execute(
        "INSERT INTO external_references (entity_type, external_id, source_system, internal_id) "
        "VALUES ('purchase_order', %s, 'ITEST', %s)",
        (external_id, node_id),
    )


def _seed_reco(
    conn,
    run_id: UUID,
    *,
    action: str,
    status: str,
    item: str,
    scenario_id: UUID = BASELINE_SCENARIO_ID,
    supplier: str | None = None,
    qty: Decimal = Decimal("10"),
    shortage: date = date(2026, 8, 1),
    proposed: date | None = None,
    current: date | None = None,
    confidence: str = "HIGH",
    target_node_id: UUID | None = None,
    source_location_id: UUID | None = None,
    dest_location_id: UUID | None = None,
    exported_at: datetime | None = None,
) -> UUID:
    reco_id = uuid4()
    conn.execute(
        """
        INSERT INTO recommendations (
            recommendation_id, agent_name, agent_run_id, scenario_id,
            item_id, item_external_id, shortage_date, deficit_qty,
            recommended_qty, supplier_external_id, action, status,
            confidence, proposed_date, current_receipt_date,
            target_node_id, source_location_id, dest_location_id, exported_at
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s
        )
        """,
        (
            reco_id, AGENT, run_id, scenario_id,
            uuid4(), item, shortage, qty,
            qty, supplier, action, status,
            confidence, proposed, current,
            target_node_id, source_location_id, dest_location_id, exported_at,
        ),
    )
    return reco_id


# ---------------------------------------------------------------------------
# Read helpers
# ---------------------------------------------------------------------------


def _export_events(conn) -> list[dict]:
    return conn.execute(
        "SELECT * FROM events WHERE event_type = 'export_executed' "
        "ORDER BY created_at"
    ).fetchall()


def _stamped_by_run(conn, run_id: UUID) -> dict[UUID, datetime]:
    rows = conn.execute(
        "SELECT recommendation_id, exported_at FROM recommendations "
        "WHERE agent_run_id = %s AND exported_at IS NOT NULL",
        (run_id,),
    ).fetchall()
    return {r["recommendation_id"]: r["exported_at"] for r in rows}


def _read_tsv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    """Read a REAL outbox file, enforcing the byte-level règles d'or on the
    way: UTF-8 without BOM, LF-only, exactly one trailing newline."""
    raw = path.read_bytes()
    assert not raw.startswith(b"\xef\xbb\xbf"), "outbox TSV must carry no BOM"
    assert b"\r" not in raw, "outbox TSV must be LF-only"
    assert raw.endswith(b"\n") and not raw.endswith(b"\n\n")
    lines = raw.decode("utf-8").splitlines()
    header = lines[0].split("\t")
    rows = [dict(zip(header, line.split("\t"))) for line in lines[1:]]
    return header, rows


# ---------------------------------------------------------------------------
# 1 + 2. Full status x family matrix -> execute_export, then immediate re-run
# ---------------------------------------------------------------------------


class TestExecuteExportMatrixAndRerun:
    def test_matrix_files_stamps_single_event_then_rerun_is_noop(self, conn, tmp_path):
        run_id = _seed_agent_run(conn)
        node_ext = _seed_node(conn)
        _seed_po_ref(conn, node_ext, f"{PREFIX}-PO-77")
        node_bare = _seed_node(conn)  # deliberately NOT in external_references
        node_cancel = _seed_node(conn)
        _seed_po_ref(conn, node_cancel, f"{PREFIX}-PO-88")
        src = _seed_location(conn, f"{PREFIX}-DC-SRC")
        dst = _seed_location(conn, f"{PREFIX}-DC-DST")

        # -- The matrix: one reco per (family action x status). ------------
        matrix: dict[tuple[str, str], UUID] = {}
        for family_action in ("ORDER_NOW", "RESCHEDULE_IN", "TRANSFER"):
            for status in ALL_STATUSES:
                kwargs: dict = {"supplier": f"{PREFIX}-SUP-1"}
                if family_action == "RESCHEDULE_IN":
                    kwargs = {
                        "target_node_id": node_ext,
                        "current": date(2026, 9, 1),
                        "proposed": date(2026, 8, 15),
                    }
                elif family_action == "TRANSFER":
                    kwargs = {
                        "source_location_id": src,
                        "dest_location_id": dst,
                    }
                matrix[(family_action, status)] = _seed_reco(
                    conn, run_id,
                    action=family_action, status=status,
                    item=f"{PREFIX}-IT-{family_action}-{status}",
                    **kwargs,
                )

        # -- Eligible coverage of the remaining actions per family. --------
        rush = _seed_reco(
            conn, run_id, action="ORDER_RUSH", status="APPROVED",
            item=f"{PREFIX}-IT-RUSH", supplier=f"{PREFIX}-SUP-2",
            qty=Decimal("33.250"),
        )
        expedite = _seed_reco(
            conn, run_id, action="EXPEDITE", status="APPLIED",
            item=f"{PREFIX}-IT-EXP", supplier=None,  # empty cell, never 'None'
        )
        resched_out = _seed_reco(
            conn, run_id, action="RESCHEDULE_OUT", status="APPROVED",
            item=f"{PREFIX}-IT-ROUT", target_node_id=node_bare,
            current=date(2026, 8, 5), proposed=date(2026, 9, 5),
        )
        cancel = _seed_reco(
            conn, run_id, action="CANCEL", status="APPLIED",
            item=f"{PREFIX}-IT-CXL", target_node_id=node_cancel,
            current=date(2026, 10, 1),
        )

        # -- Ineligible by construction, beyond the matrix. ----------------
        already = _seed_reco(
            conn, run_id, action="ORDER_NOW", status="APPROVED",
            item=f"{PREFIX}-IT-DONE", supplier=f"{PREFIX}-SUP-1",
            exported_at=PAST_TS,  # already exported — never re-exported
        )
        fork_reco = _seed_reco(
            conn, run_id, action="ORDER_NOW", status="APPROVED",
            item=f"{PREFIX}-IT-FORK", supplier=f"{PREFIX}-SUP-1",
            scenario_id=uuid4(),  # a fork's what-if — baseline-only refusal
        )

        expected_po = {
            matrix[("ORDER_NOW", "APPROVED")], matrix[("ORDER_NOW", "APPLIED")],
            rush, expedite,
        }
        expected_rs = {
            matrix[("RESCHEDULE_IN", "APPROVED")],
            matrix[("RESCHEDULE_IN", "APPLIED")],
            resched_out, cancel,
        }
        expected_tr = {
            matrix[("TRANSFER", "APPROVED")], matrix[("TRANSFER", "APPLIED")],
        }
        expected_all = expected_po | expected_rs | expected_tr

        # ------------------------------------------------------------------
        # Run 1 — the real export.
        # ------------------------------------------------------------------
        outbox1 = tmp_path / "outbox-run1"
        result = execute_export(conn, outbox1, now=NOW_1, dry_run=False)

        assert result.dry_run is False
        assert result.run_date == date(2026, 7, 18)
        assert result.files_written == (
            "po_drafts_20260718.tsv",
            "reschedule_messages_20260718.tsv",
            "transfers_20260718.tsv",
        )
        assert set(result.recommendation_ids_exported) == expected_all
        assert len(result.recommendation_ids_exported) == len(expected_all)

        # -- The REAL files: right family, right rows, règles d'or bytes. --
        po_header, po_rows = _read_tsv(outbox1 / "po_drafts_20260718.tsv")
        assert po_header == PO_HEADER
        assert {UUID(r["recommendation_id"]) for r in po_rows} == expected_po
        # The loader's ORDER BY action: EXPEDITE < ORDER_NOW < ORDER_RUSH.
        assert [r["action"] for r in po_rows] == sorted(r["action"] for r in po_rows)
        by_id = {UUID(r["recommendation_id"]): r for r in po_rows}
        assert by_id[expedite]["supplier_external_id"] == ""  # empty, not 'None'
        assert by_id[rush]["quantity"] == "33.250"

        rs_header, rs_rows = _read_tsv(outbox1 / "reschedule_messages_20260718.tsv")
        assert rs_header == RESCHEDULE_HEADER
        assert {UUID(r["recommendation_id"]) for r in rs_rows} == expected_rs
        rs_by_id = {UUID(r["recommendation_id"]): r for r in rs_rows}
        # The 3-step PO-reference chain on real joins: ERP external id when
        # external_references knows the node, raw node UUID otherwise.
        assert rs_by_id[resched_out]["target_po_reference"] == str(node_bare)
        assert rs_by_id[cancel]["target_po_reference"] == f"{PREFIX}-PO-88"
        approved_in = rs_by_id[matrix[("RESCHEDULE_IN", "APPROVED")]]
        assert approved_in["target_po_reference"] == f"{PREFIX}-PO-77"
        assert approved_in["current_receipt_date"] == "2026-09-01"
        assert approved_in["proposed_date"] == "2026-08-15"

        tr_header, tr_rows = _read_tsv(outbox1 / "transfers_20260718.tsv")
        assert tr_header == TRANSFERS_HEADER
        assert {UUID(r["recommendation_id"]) for r in tr_rows} == expected_tr
        assert {r["source_location_external_id"] for r in tr_rows} == {
            f"{PREFIX}-DC-SRC"
        }
        assert {r["dest_location_external_id"] for r in tr_rows} == {
            f"{PREFIX}-DC-DST"
        }

        # -- No ineligible id leaks into ANY file. -------------------------
        all_text = "".join(
            (outbox1 / name).read_text(encoding="utf-8")
            for name in result.files_written
        )
        for (_action, status), rid in matrix.items():
            if status not in ("APPROVED", "APPLIED"):
                assert str(rid) not in all_text
        assert str(already) not in all_text
        assert str(fork_reco) not in all_text
        assert "None" not in all_text and "NULL" not in all_text

        # -- exported_at stamped for EXACTLY the exported set. -------------
        stamped = _stamped_by_run(conn, run_id)
        assert set(stamped) == expected_all | {already}
        assert stamped[already] == PAST_TS  # pre-stamped row untouched
        for rid in expected_all:
            assert stamped[rid] == NOW_1
        fork_row = conn.execute(
            "SELECT exported_at FROM recommendations WHERE recommendation_id = %s",
            (fork_reco,),
        ).fetchone()
        assert fork_row["exported_at"] is None

        # -- Exactly ONE export_executed event, full typed contract. -------
        events = _export_events(conn)
        assert len(events) == 1
        ev = events[0]
        assert ev["event_id"] == result.event_id
        assert ev["field_changed"] == "export_executed"
        assert ev["new_date"] == date(2026, 7, 18)
        assert ev["new_quantity"] == len(expected_all)
        assert ev["new_text"] == (
            "po_drafts_20260718.tsv,reschedule_messages_20260718.tsv,"
            "transfers_20260718.tsv"
        )
        assert ev["old_text"] is None
        assert ev["scenario_id"] == BASELINE_SCENARIO_ID
        assert ev["source"] == "engine"
        assert ev["processed"] is True

        # ------------------------------------------------------------------
        # Run 2 — immediate re-run: zero file, zero stamp, zero event.
        # ------------------------------------------------------------------
        outbox2 = tmp_path / "outbox-run2"
        result2 = execute_export(
            conn, outbox2, now=NOW_1 + timedelta(hours=1), dry_run=False
        )
        assert result2.files_written == ()
        assert result2.recommendation_ids_exported == ()
        assert result2.event_id is None
        assert result2.render.files == ()
        assert not outbox2.exists()  # nothing pending -> dir never created
        assert len(_export_events(conn)) == 1  # still exactly one
        assert _stamped_by_run(conn, run_id) == stamped  # no re-stamp at all


# ---------------------------------------------------------------------------
# 3. Dry-run — zero writes, proven from a FRESH connection
# ---------------------------------------------------------------------------


class TestDryRunZeroWrites:
    def test_dry_run_writes_nothing_verified_on_fresh_connection(
        self, migrated_db, request, tmp_path
    ):
        seeded: list[UUID] = []
        with psycopg.connect(migrated_db, row_factory=dict_row) as c:
            run_id = _seed_agent_run(c)
            src = _seed_location(c, f"{PREFIX}-DRY-SRC")
            dst = _seed_location(c, f"{PREFIX}-DRY-DST")
            seeded.append(_seed_reco(
                c, run_id, action="ORDER_NOW", status="APPROVED",
                item=f"{PREFIX}-IT-DRY-1", supplier=f"{PREFIX}-SUP-DRY",
            ))
            seeded.append(_seed_reco(
                c, run_id, action="TRANSFER", status="APPLIED",
                item=f"{PREFIX}-IT-DRY-2",
                source_location_id=src, dest_location_id=dst,
            ))

            def _neutralize():
                # DEACTIVATION, never DELETE: stamp the committed pending
                # rows so no later scan (this module or another) sees them.
                with psycopg.connect(migrated_db, autocommit=True) as nc:
                    nc.execute(
                        "UPDATE recommendations SET exported_at = now() "
                        "WHERE recommendation_id = ANY(%s) AND exported_at IS NULL",
                        (seeded,),
                    )

            request.addfinalizer(_neutralize)  # registered BEFORE the commit
            c.commit()

        outbox = tmp_path / "outbox-dry"
        with psycopg.connect(migrated_db, row_factory=dict_row) as c:
            result = execute_export(
                c, outbox,
                now=datetime(2026, 7, 18, 7, 0, tzinfo=timezone.utc),
                dry_run=True,
            )
            c.commit()  # even a COMMITTED dry-run transaction leaves no trace

        assert result.dry_run is True
        assert result.files_written == ()
        assert result.recommendation_ids_exported == ()
        assert result.event_id is None
        # …while the returned preview is the REAL render.
        assert {f.filename for f in result.render.files} == {
            "po_drafts_20260718.tsv",
            "transfers_20260718.tsv",
        }
        assert set(result.render.recommendation_ids) == set(seeded)

        # Zero writes, proven from a FRESH connection.
        assert not outbox.exists()
        with psycopg.connect(migrated_db, row_factory=dict_row) as fresh:
            rows = fresh.execute(
                "SELECT exported_at FROM recommendations "
                "WHERE recommendation_id = ANY(%s)",
                (seeded,),
            ).fetchall()
            assert [r["exported_at"] for r in rows] == [None, None]
            n = fresh.execute(
                "SELECT COUNT(*) AS n FROM events WHERE event_type = 'export_executed'"
            ).fetchone()["n"]
            assert n == 0


# ---------------------------------------------------------------------------
# 4. The CLI phase's kill switch (scripts/run_daily_ingest.py)
# ---------------------------------------------------------------------------


class TestCliKillSwitch:
    def test_switch_off_apply_previews_to_stdout_and_writes_nothing(
        self, conn, tmp_path, monkeypatch, capsys
    ):
        monkeypatch.delenv("OOTILS_OUTBOUND_EXPORT_ENABLED", raising=False)
        run_id = _seed_agent_run(conn)
        rid = _seed_reco(
            conn, run_id, action="ORDER_NOW", status="APPROVED",
            item=f"{PREFIX}-IT-KS-OFF", supplier=f"{PREFIX}-SUP-KS",
        )
        outbox = tmp_path / "outbox-ks-off"

        run_daily_ingest._run_outbound_export(conn, apply=True, outbox_dir=outbox)

        assert not outbox.exists()  # no file — the dir is never even created
        row = conn.execute(
            "SELECT exported_at FROM recommendations WHERE recommendation_id = %s",
            (rid,),
        ).fetchone()
        assert row["exported_at"] is None  # no stamp
        assert _export_events(conn) == []  # no event
        out = capsys.readouterr().out
        assert "po_drafts_" in out  # the preview DID reach STDOUT
        assert str(rid) in out

        # An explicit falsy value is OFF too (same truthy-set as the other
        # kill switches).
        monkeypatch.setenv("OOTILS_OUTBOUND_EXPORT_ENABLED", "0")
        run_daily_ingest._run_outbound_export(conn, apply=True, outbox_dir=outbox)
        assert not outbox.exists()
        assert _export_events(conn) == []

    def test_switch_on_apply_writes_files_stamp_and_one_event(
        self, conn, tmp_path, monkeypatch
    ):
        monkeypatch.setenv("OOTILS_OUTBOUND_EXPORT_ENABLED", "1")
        run_id = _seed_agent_run(conn)
        rid = _seed_reco(
            conn, run_id, action="ORDER_NOW", status="APPROVED",
            item=f"{PREFIX}-IT-KS-ON", supplier=f"{PREFIX}-SUP-KS",
        )
        outbox = tmp_path / "outbox-ks-on"

        run_daily_ingest._run_outbound_export(conn, apply=True, outbox_dir=outbox)

        # The CLI phase stamps run_date from the real clock — glob the family
        # pattern instead of pinning today's date.
        files = sorted(p.name for p in outbox.glob("po_drafts_*.tsv"))
        assert len(files) == 1
        row = conn.execute(
            "SELECT exported_at FROM recommendations WHERE recommendation_id = %s",
            (rid,),
        ).fetchone()
        assert row["exported_at"] is not None
        events = _export_events(conn)
        assert len(events) == 1
        assert events[0]["new_text"] == files[0]

    def test_global_dry_run_stays_preview_even_with_switch_on(
        self, conn, tmp_path, monkeypatch, capsys
    ):
        monkeypatch.setenv("OOTILS_OUTBOUND_EXPORT_ENABLED", "1")
        run_id = _seed_agent_run(conn)
        rid = _seed_reco(
            conn, run_id, action="ORDER_NOW", status="APPROVED",
            item=f"{PREFIX}-IT-KS-DRY", supplier=f"{PREFIX}-SUP-KS",
        )
        outbox = tmp_path / "outbox-ks-dry"

        run_daily_ingest._run_outbound_export(conn, apply=False, outbox_dir=outbox)

        assert not outbox.exists()
        row = conn.execute(
            "SELECT exported_at FROM recommendations WHERE recommendation_id = %s",
            (rid,),
        ).fetchone()
        assert row["exported_at"] is None
        assert _export_events(conn) == []
        assert str(rid) in capsys.readouterr().out  # preview on STDOUT


# ---------------------------------------------------------------------------
# 5. Migration 085 — triple idempotence
# ---------------------------------------------------------------------------


class TestMigration085TripleIdempotence:
    def test_triple_execution_check_intact(self, migrated_db, conn):
        """Defensive-idempotence contract (migration 063 header, the runner
        does NOT swallow 'already exists'): triple execution overall — #1 was
        the migrated_db boot, #2 and #3 re-run the file verbatim below on an
        autocommit connection (the file carries its own BEGIN/COMMIT). Pure
        DDL ending in the same schema state — no residue to finalize."""
        sql_text = MIGRATION_085.read_text(encoding="utf-8")
        with psycopg.connect(migrated_db, autocommit=True) as raw:
            raw.execute(sql_text)  # execution #2
            raw.execute(sql_text)  # execution #3 — still a clean no-op

        # Exactly ONE CHECK survives …
        n = conn.execute(
            "SELECT COUNT(*) AS n FROM pg_constraint "
            "WHERE conname = 'events_event_type_check' "
            "  AND conrelid = 'events'::regclass"
        ).fetchone()["n"]
        assert n == 1

        # … still accepting the new type AND the pre-085 cumulative list …
        conn.execute(
            "INSERT INTO events (event_type, scenario_id) VALUES (%s, %s)",
            ("export_executed", BASELINE_SCENARIO_ID),
        )
        conn.execute(
            "INSERT INTO events (event_type, scenario_id) VALUES (%s, %s)",
            ("daily_run_completed", BASELINE_SCENARIO_ID),
        )
        # … and still rejecting garbage.
        with pytest.raises(errors.CheckViolation):
            conn.execute(
                "INSERT INTO events (event_type, scenario_id) VALUES (%s, %s)",
                ("export_definitely_unknown", BASELINE_SCENARIO_ID),
            )
        conn.rollback()
