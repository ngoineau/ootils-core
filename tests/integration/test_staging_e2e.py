"""End-to-end integration tests for the ADR-013 staging pipeline.

Each test walks the full lifecycle from upload to canonical write:

    file bytes -> /v1/staging/upload -> run_dq() -> /diff -> /approve
                                                    or /reject

These are the highest-confidence tests: they wire together every
component that landed across steps 1-9 (parser + loader + DQ L1-L4 +
diff + approve + reject) and assert the user-visible outcomes match
the design.

Run with:
    DATABASE_URL=postgresql://... pytest tests/integration/test_staging_e2e.py -v
"""
from __future__ import annotations

import io
import os
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg.rows import dict_row

from ootils_core.engine.dq.engine import run_dq

from .conftest import requires_db


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def e2e_client(migrated_db):
    os.environ["DATABASE_URL"] = migrated_db
    os.environ["OOTILS_API_TOKEN"] = "test-token"

    from fastapi.testclient import TestClient

    from ootils_core.api.app import create_app
    from ootils_core.api.dependencies import get_db
    from ootils_core.db.connection import OotilsDB

    app = create_app()
    db = OotilsDB(migrated_db)

    def override_db():
        with db.conn() as c:
            yield c

    app.dependency_overrides[get_db] = override_db
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()


@pytest.fixture
def auth():
    return {"Authorization": "Bearer test-token"}


@pytest.fixture
def conn(migrated_db: str):
    with psycopg.connect(migrated_db, row_factory=dict_row) as c:
        yield c
        c.rollback()


# ---------------------------------------------------------------------------
# Helpers — the canonical "happy" pipeline drive
# ---------------------------------------------------------------------------


def _upload(client, auth, content: bytes, entity_type: str, source_system: str,
            filename: str = "data.tsv") -> UUID:
    resp = client.post(
        "/v1/staging/upload",
        headers=auth,
        files={"file": (filename, io.BytesIO(content), "text/plain")},
        data={"entity_type": entity_type, "source_system": source_system},
    )
    assert resp.status_code == 202, resp.text
    return UUID(resp.json()["batch_id"])


def _run_dq(conn, batch_id: UUID):
    """Invoke the DQ engine directly (in real ops a worker / event-driven
    runner would trigger this after /upload returns)."""
    return run_dq(conn, batch_id)


# ---------------------------------------------------------------------------
# E2E #1 — Happy path: clean TSV all the way through to canonical
# ---------------------------------------------------------------------------


@requires_db
def test_e2e_clean_tsv_lands_in_canonical(e2e_client, auth, conn) -> None:
    """The canonical end-to-end test: upload a clean TSV, run DQ,
    review the diff, approve, and verify the canonical items table."""
    src = "E2E-HAPPY-" + uuid4().hex[:6]

    # ---- 1. Upload ----
    tsv = (
        b"external_id\tname\titem_type\tuom\tstatus\n"
        b"E2E-001\tWidget A\tfinished_good\tEA\tactive\n"
        b"E2E-002\tWidget B\tcomponent\tEA\tactive\n"
        b"E2E-003\tCable\tcomponent\tM\tactive\n"
    )
    batch_id = _upload(e2e_client, auth, tsv, "items", src)

    # Batch is 'pending', no DQ yet
    row = conn.execute(
        "SELECT status, dq_status FROM ingest_batches WHERE batch_id = %s",
        (batch_id,),
    ).fetchone()
    assert row["status"] == "pending"

    # ---- 2. Run DQ (L1-L4) ----
    dq_result = _run_dq(conn, batch_id)
    assert dq_result.batch_dq_status in ("validated",), (
        f"unexpected DQ status: {dq_result.batch_dq_status} "
        f"issues={[(i.rule_code, i.severity, i.message) for i in dq_result.issues]}"
    )
    assert dq_result.passed_rows == 3
    assert dq_result.failed_rows == 0

    # Promote pending -> validated. In real ops the DQ engine would do
    # this transition itself; here the engine only flips dq_status,
    # so we mimic the orchestrator's role.
    conn.execute(
        "UPDATE ingest_batches SET status = 'validated' WHERE batch_id = %s",
        (batch_id,),
    )
    conn.commit()

    # ---- 3. Review diff ----
    diff = e2e_client.get(
        f"/v1/staging/batches/{batch_id}/diff", headers=auth,
    ).json()
    assert diff["supported"] is True
    assert diff["counts"]["will_insert"] == 3
    assert diff["counts"]["will_update"] == 0
    assert diff["counts"]["will_soft_delete"] == 0
    assert diff["deletion_guard"]["exceeds_threshold"] is False

    # ---- 4. Approve ----
    approve_resp = e2e_client.post(
        f"/v1/staging/batches/{batch_id}/approve",
        headers=auth,
        json={"approved_by": "e2e@example.com", "notes": "happy path test"},
    )
    assert approve_resp.status_code == 200, approve_resp.text
    body = approve_resp.json()
    assert body["counts"]["rows_inserted"] == 3
    assert body["counts"]["rows_updated"] == 0
    assert body["counts"]["rows_soft_deleted"] == 0

    # ---- 5. Verify canonical items table + lifecycle ----
    items = conn.execute(
        "SELECT external_id, name, status FROM items "
        "WHERE external_id LIKE 'E2E-%%' ORDER BY external_id"
    ).fetchall()
    assert len(items) == 3
    assert items[0]["name"] == "Widget A"
    assert items[0]["status"] == "active"

    # Batch reached terminal status
    final = conn.execute(
        "SELECT status, imported_at FROM ingest_batches WHERE batch_id = %s",
        (batch_id,),
    ).fetchone()
    assert final["status"] == "imported"
    assert final["imported_at"] is not None

    # Audit row in transform_runs
    audit = conn.execute(
        "SELECT status, rows_inserted, approved_by FROM staging.transform_runs "
        "WHERE batch_id = %s",
        (batch_id,),
    ).fetchone()
    assert audit["status"] == "completed"
    assert audit["rows_inserted"] == 3
    assert audit["approved_by"] == "e2e@example.com"


# ---------------------------------------------------------------------------
# E2E #2 — DQ catches a bad row -> batch rejected, can't approve
# ---------------------------------------------------------------------------


@requires_db
def test_e2e_dq_blocks_bad_data(e2e_client, auth, conn) -> None:
    """An items TSV with an invalid item_type must be rejected by DQ
    (L3_INVALID_ITEM_TYPE) and the resulting batch cannot be approved."""
    src = "E2E-BAD-" + uuid4().hex[:6]

    # 'gadget' is not in the valid item_type enum
    tsv = (
        b"external_id\tname\titem_type\tuom\tstatus\n"
        b"E2E-BAD-001\tThingamajig\tgadget\tEA\tactive\n"
    )
    batch_id = _upload(e2e_client, auth, tsv, "items", src)

    dq_result = _run_dq(conn, batch_id)
    # L3 fires here — at least one error issue
    rule_codes = [i.rule_code for i in dq_result.issues]
    assert "L3_INVALID_ITEM_TYPE" in rule_codes
    assert dq_result.batch_dq_status == "rejected"

    # Attempt approval — must be refused
    resp = e2e_client.post(
        f"/v1/staging/batches/{batch_id}/approve",
        headers=auth,
        json={"approved_by": "e2e@example.com"},
    )
    assert resp.status_code == 400
    assert "validated" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# E2E #3 — Reject path (operator chooses NOT to approve a DQ-clean batch)
# ---------------------------------------------------------------------------


@requires_db
def test_e2e_reject_a_validated_batch(e2e_client, auth, conn) -> None:
    """Operator uploads a clean batch, runs DQ, reviews diff, but decides
    not to approve (e.g. diff showed unexpected soft-delete count).
    The /reject path closes out the lifecycle cleanly."""
    src = "E2E-REJ-" + uuid4().hex[:6]

    tsv = (
        b"external_id\tname\titem_type\tuom\tstatus\n"
        b"E2E-REJ-001\tIffy Item\tcomponent\tEA\tactive\n"
    )
    batch_id = _upload(e2e_client, auth, tsv, "items", src)
    _run_dq(conn, batch_id)
    conn.execute(
        "UPDATE ingest_batches SET status = 'validated' WHERE batch_id = %s",
        (batch_id,),
    )
    conn.commit()

    # Operator rejects after diff review
    resp = e2e_client.post(
        f"/v1/staging/batches/{batch_id}/reject",
        headers=auth,
        json={"rejected_by": "e2e@example.com",
              "reason": "/diff showed unexpected impact, going back to source"},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["new_status"] == "rejected"

    # No canonical write happened
    items = conn.execute(
        "SELECT COUNT(*) AS n FROM items WHERE external_id = 'E2E-REJ-001'"
    ).fetchone()
    assert items["n"] == 0

    # Audit row in transform_runs marks the rollback
    audit = conn.execute(
        "SELECT status, approval_notes FROM staging.transform_runs WHERE batch_id = %s",
        (batch_id,),
    ).fetchone()
    assert audit["status"] == "rolled_back"
    assert "unexpected impact" in audit["approval_notes"]


# ---------------------------------------------------------------------------
# E2E #4 — Update + soft-delete in a 2nd batch from the same source
# ---------------------------------------------------------------------------


@requires_db
def test_e2e_second_batch_updates_and_soft_deletes(e2e_client, auth, conn) -> None:
    """The most realistic ops pattern: source pushes batch 1 (initial
    load), then batch 2 (with one update + one drop). Verify the second
    approval correctly updates + soft-deletes canonical state."""
    src = "E2E-2BATCH-" + uuid4().hex[:6]

    # ---- Batch 1: 4 fresh items ----
    tsv1 = (
        b"external_id\tname\titem_type\tuom\tstatus\n"
        b"E2E-2B-001\tA\tcomponent\tEA\tactive\n"
        b"E2E-2B-002\tB\tcomponent\tEA\tactive\n"
        b"E2E-2B-003\tC\tcomponent\tEA\tactive\n"
        b"E2E-2B-004\tD\tcomponent\tEA\tactive\n"
    )
    b1 = _upload(e2e_client, auth, tsv1, "items", src)
    _run_dq(conn, b1)
    conn.execute(
        "UPDATE ingest_batches SET status = 'validated' WHERE batch_id = %s", (b1,)
    )
    conn.commit()
    e2e_client.post(
        f"/v1/staging/batches/{b1}/approve",
        headers=auth,
        json={"approved_by": "e2e@example.com"},
    )

    # ---- Batch 2: D is gone, B got a new name, A and C stay the same ----
    tsv2 = (
        b"external_id\tname\titem_type\tuom\tstatus\n"
        b"E2E-2B-001\tA\tcomponent\tEA\tactive\n"          # noop
        b"E2E-2B-002\tB renamed\tcomponent\tEA\tactive\n"   # update
        b"E2E-2B-003\tC\tcomponent\tEA\tactive\n"          # noop
        # E2E-2B-004 missing -> soft-delete
    )
    b2 = _upload(e2e_client, auth, tsv2, "items", src)
    _run_dq(conn, b2)
    conn.execute(
        "UPDATE ingest_batches SET status = 'validated' WHERE batch_id = %s", (b2,)
    )
    conn.commit()

    # 1 deletion of 4 = 25% > 20% guard -> need force
    resp = e2e_client.post(
        f"/v1/staging/batches/{b2}/approve",
        headers=auth,
        json={"approved_by": "e2e@example.com"},
    )
    assert resp.status_code == 400
    assert "deletion ratio" in resp.json()["detail"]

    # Retry with force=true
    resp = e2e_client.post(
        f"/v1/staging/batches/{b2}/approve",
        headers=auth,
        json={"approved_by": "e2e@example.com",
              "notes": "D was decommissioned; confirmed",
              "force": True},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["counts"]["rows_updated"] == 1   # B
    assert body["counts"]["rows_noop"] == 2      # A, C
    assert body["counts"]["rows_soft_deleted"] == 1  # D
    assert body["forced_approval"] is True

    # Verify canonical state
    b_row = conn.execute(
        "SELECT name, status FROM items WHERE external_id = 'E2E-2B-002'"
    ).fetchone()
    assert b_row["name"] == "B renamed"

    d_row = conn.execute(
        "SELECT status FROM items WHERE external_id = 'E2E-2B-004'"
    ).fetchone()
    assert d_row["status"] == "obsolete"


# ---------------------------------------------------------------------------
# E2E #5 — Source isolation: two sources don't interfere
# ---------------------------------------------------------------------------


@requires_db
def test_e2e_two_sources_dont_interfere(e2e_client, auth, conn) -> None:
    """SAP and MES push the same item_id range; each is scoped by its
    source_system. Approving one must not soft-delete the other's footprint."""
    src_sap = "E2E-SAP-" + uuid4().hex[:6]
    src_mes = "E2E-MES-" + uuid4().hex[:6]

    # SAP pushes 3 items
    sap_tsv = (
        b"external_id\tname\titem_type\tuom\tstatus\n"
        b"E2E-ISO-001\tFromSAP-1\tcomponent\tEA\tactive\n"
        b"E2E-ISO-002\tFromSAP-2\tcomponent\tEA\tactive\n"
    )
    sap_batch = _upload(e2e_client, auth, sap_tsv, "items", src_sap)
    _run_dq(conn, sap_batch)
    conn.execute(
        "UPDATE ingest_batches SET status = 'validated' WHERE batch_id = %s",
        (sap_batch,),
    )
    conn.commit()
    e2e_client.post(
        f"/v1/staging/batches/{sap_batch}/approve",
        headers=auth, json={"approved_by": "e2e@x"},
    )

    # MES pushes a single item with a different external_id
    mes_tsv = (
        b"external_id\tname\titem_type\tuom\tstatus\n"
        b"E2E-ISO-MES-001\tFromMES\tcomponent\tEA\tactive\n"
    )
    mes_batch = _upload(e2e_client, auth, mes_tsv, "items", src_mes)
    _run_dq(conn, mes_batch)
    conn.execute(
        "UPDATE ingest_batches SET status = 'validated' WHERE batch_id = %s",
        (mes_batch,),
    )
    conn.commit()

    # Diff for MES must NOT include SAP's items as soft-delete candidates
    diff = e2e_client.get(
        f"/v1/staging/batches/{mes_batch}/diff", headers=auth,
    ).json()
    assert diff["counts"]["will_soft_delete"] == 0
    assert diff["counts"]["will_insert"] == 1

    # Approve MES
    e2e_client.post(
        f"/v1/staging/batches/{mes_batch}/approve",
        headers=auth, json={"approved_by": "e2e@x"},
    )

    # SAP's items must still be active
    sap_items = conn.execute(
        "SELECT external_id, status FROM items WHERE external_id LIKE 'E2E-ISO-%%' "
        "ORDER BY external_id"
    ).fetchall()
    assert len(sap_items) == 3  # 2 SAP + 1 MES
    assert all(r["status"] == "active" for r in sap_items)
