"""
tests/integration/test_reconciliation_integration.py — the heuristic
reconciliation WRITE path (``engine.reconciliation.matcher.run_reconciliation``,
ADR-042 decision 4, PR-5b) against a real PostgreSQL — no DB mocks (CLAUDE.md).
The pure matcher lives in ``tests/test_reconciliation_matcher.py``.

Re-derived against the REAL contract (matcher.py is the authority): the sole
writer is ``run_reconciliation(conn, run_date, *, now=None, dry_run=False)`` —
``run_date`` is an explicit positional (stamped onto the ``reconciliation_runs``
row), ``now`` is the deterministic stamp clock, ``dry_run`` is the read-only
preview. There is NO ``OOTILS_RECONCILIATION_ENABLED`` kill switch in the
contract (the earlier draft asserted one that does not exist); ``dry_run=True``
is the real "compute-but-write-nothing" mechanism.

Four axes, each asserting on DB STATE (migration 086's schema + emit.py's typed
``reconciliation_completed`` contract) as well as on the returned
``ReconciliationRunResult``, so the coupling is to the CONTRACT:

  1 + 3. MATCH -> STAMP + RUN + EVENT, STATE MACHINE UNTOUCHED, then IDEMPOTENT
     RE-RUN. An exported, not-yet-reconciled baseline reco + an inbound PO
     created AFTER the export (same item, qty within +/-5%, date within +/-7d)
     is paired: the reco gets ``fulfilled_at`` + ``fulfilled_erp_id`` stamped (an
     OBSERVATION — its ``status`` is UNTOUCHED, the state machine is never
     driven), exactly ONE ``reconciliation_runs`` row is written
     (candidates/matched/ambiguous/unmatched honest), and exactly ONE
     ``reconciliation_completed`` event carries the full typed contract
     (field_changed / new_quantity=matched / new_text=run_id /
     old_text='ambiguous=N,unmatched=N'; baseline scenario, source 'engine').
     A re-run with no new PO stamps NOTHING new (the ``fulfilled_at IS NULL``
     scan already excludes the fulfilled reco, so it is no longer even a
     candidate) yet still writes a NEW run row + event (append-only ledger,
     ADR-042 decision 4: "un run = UNE ligne + UN event").

  2. PO PRE-DATES THE EXPORT -> UNMATCHED. A PO whose ``nodes.created_at`` is
     BEFORE the reco's ``exported_at`` (a pre-existing/upserted PO, not a
     genuine fulfilment of THIS recommendation) is not plausible: the reco stays
     ``fulfilled_at IS NULL``, the run row counts it ``unmatched``, the event is
     still emitted with ``new_quantity=0``.

  4. DRY-RUN -> ZERO WRITES. ``run_reconciliation(..., dry_run=True)`` computes
     the identical match (the returned result still reports matched=1) but
     writes NOTHING: no stamp, no ``reconciliation_runs`` row, no event
     (``run_id``/``event_id`` are None) — the CLI's read-only preview.

ISOLATION (pattern of ``test_outbound_export_integration.py``): a unique
``PREFIX`` scopes every seeded item/PO so no foreign row can collide, and an
autouse module sweep (i) surgically DELETEs committed
``reconciliation_completed`` events and (ii) NEUTRALIZES foreign committed
reconciliation candidates (exported, not-yet-reconciled baseline recos) by
stamping their ``fulfilled_at`` — DEACTIVATION, never a DELETE of governed audit
rows (ADR-039). MY seeded reco is then the only candidate the matcher can scan.
Every test rides the rollback-teardown ``conn`` fixture (``run_reconciliation``
never commits by contract, same as ``execute_export``), so its run rows / events
/ stamps are swept away with the transaction; ``reconciliation_runs`` and event
counts are asserted as DELTAS so a foreign committed row never perturbs the
assertion.

Requires a live PostgreSQL (``requires_db``).
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID, uuid4

import psycopg
import pytest

from ootils_core.constants import BASELINE_SCENARIO_ID
from ootils_core.engine.reconciliation.matcher import run_reconciliation

from .conftest import requires_db

pytestmark = requires_db

PREFIX = f"RCN-{uuid4().hex[:8]}"
AGENT = f"reconciliation-itest-{PREFIX}"

# Deterministic clock + explicit run_date: caller-supplied, never an internal
# clock (repo convention — execute_export / record_daily_run both take an
# explicit now; run_reconciliation additionally takes an explicit run_date).
NOW = datetime(2026, 7, 18, 6, 30, tzinfo=timezone.utc)
RUN_DATE = date(2026, 7, 18)
EXPORTED_AT = datetime(2026, 7, 16, 5, 0, tzinfo=timezone.utc)  # 2 days before NOW
PO_CREATED_AFTER = datetime(2026, 7, 17, 5, 0, tzinfo=timezone.utc)  # after export, before now


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module", autouse=True)
def _module_sweep(migrated_db):
    """Belt-and-braces pre-clean (pattern of
    ``test_outbound_export_integration.py``): surgical DELETE of committed
    ``reconciliation_completed`` events + stamp-only NEUTRALIZATION of foreign
    committed reconciliation candidates so MY seeded reco is the only one the
    matcher scans — never a DELETE of governed audit rows (ADR-039)."""
    with psycopg.connect(migrated_db, autocommit=True) as c:
        c.execute("DELETE FROM events WHERE event_type = 'reconciliation_completed'")
        c.execute(
            "UPDATE recommendations SET fulfilled_at = now(), "
            "fulfilled_erp_id = 'itest-swept' "
            "WHERE scenario_id = %s AND exported_at IS NOT NULL "
            "  AND fulfilled_at IS NULL",
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


def _seed_item(conn, external_id: str) -> UUID:
    item_id = uuid4()
    conn.execute(
        "INSERT INTO items (item_id, name, external_id) VALUES (%s, %s, %s)",
        (item_id, external_id, external_id),
    )
    return item_id


def _seed_location(conn, external_id: str) -> UUID:
    loc_id = uuid4()
    conn.execute(
        "INSERT INTO locations (location_id, name, external_id) VALUES (%s, %s, %s)",
        (loc_id, external_id, external_id),
    )
    return loc_id


def _seed_candidate_reco(
    conn,
    agent_run_id: UUID,
    *,
    item_id: UUID,
    item_external_id: str,
    exported_at: datetime,
    action: str = "ORDER_NOW",
    status: str = "APPROVED",
    qty: Decimal = Decimal("100"),
    shortage: date = date(2026, 8, 1),
    supplier: str | None = None,
    source_location_id: UUID | None = None,
    dest_location_id: UUID | None = None,
) -> UUID:
    """An exported, not-yet-reconciled baseline reco — a reconciliation
    candidate (``exported_at IS NOT NULL AND fulfilled_at IS NULL``, migration
    086's ``ix_reco_pending_reconciliation`` predicate). ``fulfilled_at`` /
    ``fulfilled_erp_id`` are left at their NULL default (a fresh candidate)."""
    reco_id = uuid4()
    conn.execute(
        """
        INSERT INTO recommendations (
            recommendation_id, agent_name, agent_run_id, scenario_id,
            item_id, item_external_id, shortage_date, deficit_qty,
            recommended_qty, supplier_external_id, action, status,
            confidence, source_location_id, dest_location_id, exported_at
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s
        )
        """,
        (
            reco_id, AGENT, agent_run_id, BASELINE_SCENARIO_ID,
            item_id, item_external_id, shortage, qty,
            qty, supplier, action, status,
            "HIGH", source_location_id, dest_location_id, exported_at,
        ),
    )
    return reco_id


def _seed_inbound_po(
    conn,
    *,
    erp_id: str,
    item_id: UUID,
    location_id: UUID,
    qty: Decimal,
    delivery_date: date,
    created_at: datetime,
) -> UUID:
    """A baseline inbound ERP PO exactly as ``POST /v1/ingest/purchase-orders``
    lands it (``ingest.py``): a ``PurchaseOrderSupply`` node
    (``time_ref`` = expected delivery, ``quantity``, explicit ``created_at`` so
    the created-after-export heuristic is exercisable) keyed by its ERP number
    through ``external_references`` (entity_type ``purchase_order``). No supplier
    is persisted on the node (KNOWN GAP 1) — ``_load_inbound_pos`` loads it as
    NULL."""
    node_id = uuid4()
    conn.execute(
        """
        INSERT INTO nodes (
            node_id, node_type, scenario_id, item_id, location_id,
            quantity, qty_uom, time_grain, time_ref, active, created_at
        ) VALUES (
            %s, 'PurchaseOrderSupply', %s, %s, %s,
            %s, 'EA', 'exact_date', %s, TRUE, %s
        )
        """,
        (node_id, BASELINE_SCENARIO_ID, item_id, location_id, qty, delivery_date, created_at),
    )
    conn.execute(
        "INSERT INTO external_references (entity_type, external_id, source_system, internal_id) "
        "VALUES ('purchase_order', %s, 'ITEST', %s)",
        (erp_id, node_id),
    )
    return node_id


# ---------------------------------------------------------------------------
# Read helpers
# ---------------------------------------------------------------------------


def _run_ids(conn) -> set[UUID]:
    return {
        r["run_id"]
        for r in conn.execute("SELECT run_id FROM reconciliation_runs").fetchall()
    }


def _run_row(conn, run_id: UUID) -> dict:
    return conn.execute(
        "SELECT * FROM reconciliation_runs WHERE run_id = %s", (run_id,)
    ).fetchone()


def _reco(conn, reco_id: UUID) -> dict:
    return conn.execute(
        "SELECT * FROM recommendations WHERE recommendation_id = %s", (reco_id,)
    ).fetchone()


def _recon_event_for_run(conn, run_id: UUID) -> dict | None:
    return conn.execute(
        "SELECT * FROM events WHERE event_type = 'reconciliation_completed' "
        "AND new_text = %s",
        (str(run_id),),
    ).fetchone()


def _recon_event_count(conn) -> int:
    return conn.execute(
        "SELECT COUNT(*) AS n FROM events WHERE event_type = 'reconciliation_completed'"
    ).fetchone()["n"]


# ---------------------------------------------------------------------------
# 1 + 3. Match -> stamp + run + event; state machine untouched; idempotent re-run
# ---------------------------------------------------------------------------


class TestReconcileStampRunEventAndRerun:
    def test_match_stamps_writes_run_emits_event_then_rerun_is_noop(self, conn):
        agent = _seed_agent_run(conn)
        item_ext = f"{PREFIX}-IT-RECON"
        item_id = _seed_item(conn, item_ext)
        loc_id = _seed_location(conn, f"{PREFIX}-DC")
        erp = f"{PREFIX}-PO-RECON"

        reco = _seed_candidate_reco(
            conn, agent, item_id=item_id, item_external_id=item_ext,
            action="ORDER_NOW", status="APPROVED",
            qty=Decimal("100"), shortage=date(2026, 8, 1), exported_at=EXPORTED_AT,
        )
        _seed_inbound_po(
            conn, erp_id=erp, item_id=item_id, location_id=loc_id,
            qty=Decimal("100"), delivery_date=date(2026, 8, 1),
            created_at=PO_CREATED_AFTER,
        )

        # ------------------------------------------------------------------
        # Run 1 — the real reconciliation pass.
        # ------------------------------------------------------------------
        before = _run_ids(conn)
        result = run_reconciliation(conn, RUN_DATE, now=NOW)
        new = _run_ids(conn) - before
        assert len(new) == 1  # exactly one append-only run row
        run_id = next(iter(new))

        # -- The returned ReconciliationRunResult mirrors the persisted row. --
        assert result.run_id == run_id
        assert result.run_date == RUN_DATE
        assert result.candidates == 1
        assert result.matched == 1
        assert result.ambiguous == 0
        assert result.unmatched == 0
        assert result.event_id is not None
        assert result.match.matched == [(reco, erp)]

        # -- The append-only run ledger row: honest tallies. ---------------
        row = _run_row(conn, run_id)
        assert row["run_date"] == RUN_DATE
        assert row["recos_candidates"] == 1
        assert row["matched"] == 1
        assert row["ambiguous"] == 0
        assert row["unmatched"] == 0

        # -- The reco: STAMPED as observation, state machine UNTOUCHED. -----
        r = _reco(conn, reco)
        assert r["fulfilled_at"] == NOW  # caller's clock, deterministic
        assert r["fulfilled_erp_id"] == erp  # the inbound PO's ERP number
        assert r["status"] == "APPROVED"  # NEVER driven by reconciliation

        # -- Exactly ONE reconciliation_completed event, full typed contract.
        ev = _recon_event_for_run(conn, run_id)
        assert ev is not None
        assert ev["event_id"] == result.event_id
        assert ev["field_changed"] == "reconciliation_completed"
        assert ev["new_quantity"] == 1  # matched
        assert ev["new_text"] == str(run_id)  # companion-table run ref
        assert ev["old_text"] == "ambiguous=0,unmatched=0"
        # …and old_text is CONSISTENT with the run row.
        assert ev["old_text"] == (
            f"ambiguous={row['ambiguous']},unmatched={row['unmatched']}"
        )
        assert ev["new_date"] is None  # not part of the reconciliation contract
        assert ev["scenario_id"] == BASELINE_SCENARIO_ID  # baseline-only
        assert ev["source"] == "engine"
        assert ev["processed"] is True

        # ------------------------------------------------------------------
        # Run 2 — re-run with no new PO: zero new stamp, a NEW run row + event.
        # ------------------------------------------------------------------
        before2 = _run_ids(conn)
        events_before2 = _recon_event_count(conn)
        fulfilled_before2 = _reco(conn, reco)["fulfilled_at"]

        result2 = run_reconciliation(conn, RUN_DATE, now=NOW + timedelta(hours=1))

        new2 = _run_ids(conn) - before2
        assert len(new2) == 1  # a NEW, legitimate append-only run row
        assert result2.run_id == next(iter(new2))
        row2 = _run_row(conn, result2.run_id)
        assert row2["matched"] == 0
        assert row2["recos_candidates"] == 0  # the only candidate is fulfilled now
        assert result2.candidates == 0
        assert result2.matched == 0

        # ZERO new stamp — the fulfilled_at IS NULL scan already excludes it.
        r2 = _reco(conn, reco)
        assert r2["fulfilled_at"] == fulfilled_before2  # untouched
        assert r2["fulfilled_erp_id"] == erp
        # One event per run (ADR-042 decision 4 / emit.py: always emitted).
        assert _recon_event_count(conn) == events_before2 + 1


# ---------------------------------------------------------------------------
# 2. PO pre-dates the export -> unmatched, nothing stamped
# ---------------------------------------------------------------------------


class TestPoBeforeExportIsUnmatched:
    def test_po_created_before_export_leaves_reco_unmatched(self, conn):
        agent = _seed_agent_run(conn)
        item_ext = f"{PREFIX}-IT-BEFORE"
        item_id = _seed_item(conn, item_ext)
        loc_id = _seed_location(conn, f"{PREFIX}-DC-BEFORE")

        reco = _seed_candidate_reco(
            conn, agent, item_id=item_id, item_external_id=item_ext,
            action="ORDER_NOW", status="APPROVED",
            qty=Decimal("100"), shortage=date(2026, 8, 1), exported_at=EXPORTED_AT,
        )
        # created 3 days BEFORE the export -> a pre-existing PO, not a fulfilment.
        _seed_inbound_po(
            conn, erp_id=f"{PREFIX}-PO-OLD", item_id=item_id, location_id=loc_id,
            qty=Decimal("100"), delivery_date=date(2026, 8, 1),
            created_at=EXPORTED_AT - timedelta(days=3),
        )

        before = _run_ids(conn)
        result = run_reconciliation(conn, RUN_DATE, now=NOW)
        run_id = result.run_id
        assert run_id in (_run_ids(conn) - before)

        assert result.candidates == 1
        assert result.matched == 0
        assert result.unmatched == 1

        row = _run_row(conn, run_id)
        assert row["recos_candidates"] == 1
        assert row["matched"] == 0
        assert row["unmatched"] == 1

        # The reco is NOT stamped — no silent match.
        r = _reco(conn, reco)
        assert r["fulfilled_at"] is None
        assert r["fulfilled_erp_id"] is None

        # The event is still emitted (one run = one event), with matched=0.
        ev = _recon_event_for_run(conn, run_id)
        assert ev is not None
        assert ev["new_quantity"] == 0
        assert ev["old_text"] == "ambiguous=0,unmatched=1"


# ---------------------------------------------------------------------------
# 4. Dry-run -> computes the match but writes NOTHING
# ---------------------------------------------------------------------------


class TestDryRunWritesNothing:
    def test_dry_run_computes_match_but_writes_nothing(self, conn):
        agent = _seed_agent_run(conn)
        item_ext = f"{PREFIX}-IT-DRY"
        item_id = _seed_item(conn, item_ext)
        loc_id = _seed_location(conn, f"{PREFIX}-DC-DRY")

        reco = _seed_candidate_reco(
            conn, agent, item_id=item_id, item_external_id=item_ext,
            action="ORDER_NOW", status="APPROVED",
            qty=Decimal("100"), shortage=date(2026, 8, 1), exported_at=EXPORTED_AT,
        )
        _seed_inbound_po(
            conn, erp_id=f"{PREFIX}-PO-DRY", item_id=item_id, location_id=loc_id,
            qty=Decimal("100"), delivery_date=date(2026, 8, 1),
            created_at=PO_CREATED_AFTER,
        )

        runs_before = _run_ids(conn)
        events_before = _recon_event_count(conn)

        result = run_reconciliation(conn, RUN_DATE, now=NOW, dry_run=True)

        # The match IS computed and returned — the preview is real …
        assert result.candidates == 1
        assert result.matched == 1
        assert result.match.matched == [(reco, f"{PREFIX}-PO-DRY")]
        # … but nothing is persisted: no run id, no event id.
        assert result.run_id is None
        assert result.event_id is None

        # No stamp, no new run row, no new event — the dry-run contract.
        assert _reco(conn, reco)["fulfilled_at"] is None
        assert _reco(conn, reco)["fulfilled_erp_id"] is None
        assert _run_ids(conn) == runs_before
        assert _recon_event_count(conn) == events_before
