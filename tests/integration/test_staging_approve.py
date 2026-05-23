"""Integration tests for POST /v1/staging/batches/{id}/approve.

End-to-end pipeline: upload TSV -> validate -> approve -> verify the
canonical tables changed as the diff predicted.

These tests exercise the full ADR-013 flow that this PR completes:
the user-visible workflow (upload, validate, approve) plus the
invariant that after approval the canonical state matches what the
batch dictated, scoped to the source_system.
"""
from __future__ import annotations

import io
import os
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg.rows import dict_row

from .conftest import requires_db


@pytest.fixture(scope="module")
def staging_client(migrated_db):
    """Module-scoped TestClient sharing the migrated DB."""
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
# Helpers
# ---------------------------------------------------------------------------


def _upload_and_validate(
    staging_client, auth, conn, rows, source_system,
) -> UUID:
    """Upload a TSV + force the batch into 'validated' state.

    The /upload endpoint leaves the batch at status='pending'. In real
    operation the DQ runner moves it to 'validated' (or 'rejected').
    For these tests we skip the DQ run and set the status directly so
    the approval path is the test target.
    """
    lines = ["external_id\tname\titem_type\tuom\tstatus"]
    for r in rows:
        lines.append("\t".join(r))
    data = ("\n".join(lines) + "\n").encode("utf-8")
    resp = staging_client.post(
        "/v1/staging/upload",
        headers=auth,
        files={"file": ("items.tsv", io.BytesIO(data), "text/plain")},
        data={"entity_type": "items", "source_system": source_system},
    )
    assert resp.status_code == 202, resp.text
    batch_id = UUID(resp.json()["batch_id"])
    conn.execute(
        "UPDATE ingest_batches SET status = 'validated', dq_status = 'validated' "
        "WHERE batch_id = %s",
        (batch_id,),
    )
    conn.commit()
    return batch_id


def _seed_canonical_item(
    conn, external_id: str, name: str, source_system: str,
    item_type: str = "component", uom: str = "EA", status: str = "active",
) -> UUID:
    item_id = uuid4()
    conn.execute(
        """
        INSERT INTO items (item_id, external_id, name, item_type, uom, status)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (external_id) DO NOTHING
        """,
        (item_id, external_id, name, item_type, uom, status),
    )
    conn.execute(
        """
        INSERT INTO external_references
            (entity_type, external_id, internal_id, source_system)
        VALUES ('item', %s, %s, %s)
        ON CONFLICT (entity_type, external_id, source_system) DO NOTHING
        """,
        (external_id, item_id, source_system),
    )
    conn.commit()
    return item_id


# ---------------------------------------------------------------------------
# Pure insert
# ---------------------------------------------------------------------------


@requires_db
def test_approve_pure_insert_creates_canonical_rows(
    staging_client, auth, conn,
) -> None:
    src = "APPROVE-INS-" + uuid4().hex[:6]
    batch_id = _upload_and_validate(
        staging_client, auth, conn,
        [
            ("APP-INS-001", "New One", "component", "EA", "active"),
            ("APP-INS-002", "New Two", "component", "EA", "active"),
        ],
        source_system=src,
    )

    resp = staging_client.post(
        f"/v1/staging/batches/{batch_id}/approve",
        headers=auth,
        json={"approved_by": "test@example.com", "notes": "first import"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["counts"]["rows_inserted"] == 2
    assert body["counts"]["rows_updated"] == 0
    assert body["counts"]["rows_soft_deleted"] == 0
    assert body["forced_approval"] is False

    # Verify canonical tables now hold the rows
    rows = conn.execute(
        "SELECT external_id, name FROM items WHERE external_id LIKE 'APP-INS-%' "
        "ORDER BY external_id"
    ).fetchall()
    assert len(rows) == 2
    assert rows[0]["name"] == "New One"

    # Verify external_references rows exist for this source
    refs = conn.execute(
        "SELECT external_id FROM external_references "
        "WHERE source_system = %s ORDER BY external_id",
        (src,),
    ).fetchall()
    assert {r["external_id"] for r in refs} == {"APP-INS-001", "APP-INS-002"}

    # Batch transitioned to 'imported'
    batch = conn.execute(
        "SELECT status, imported_at FROM ingest_batches WHERE batch_id = %s",
        (batch_id,),
    ).fetchone()
    assert batch["status"] == "imported"
    assert batch["imported_at"] is not None


# ---------------------------------------------------------------------------
# Update flow
# ---------------------------------------------------------------------------


@requires_db
def test_approve_updates_existing_row(staging_client, auth, conn) -> None:
    src = "APPROVE-UPD-" + uuid4().hex[:6]
    _seed_canonical_item(conn, "APP-UPD-001", "Old Name", src)

    batch_id = _upload_and_validate(
        staging_client, auth, conn,
        [("APP-UPD-001", "New Name", "component", "EA", "active")],
        source_system=src,
    )

    resp = staging_client.post(
        f"/v1/staging/batches/{batch_id}/approve",
        headers=auth,
        json={"approved_by": "test@example.com"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["counts"]["rows_inserted"] == 0
    assert body["counts"]["rows_updated"] == 1

    # The canonical row reflects the new name
    item = conn.execute(
        "SELECT name FROM items WHERE external_id = 'APP-UPD-001'"
    ).fetchone()
    assert item["name"] == "New Name"


# ---------------------------------------------------------------------------
# Soft-delete
# ---------------------------------------------------------------------------


@requires_db
def test_approve_soft_deletes_missing_rows(staging_client, auth, conn) -> None:
    """An item present in canonical for this source but absent from the
    batch must end up status='obsolete' AND its external_references
    mapping must be removed for this source."""
    src = "APPROVE-DEL-" + uuid4().hex[:6]
    _seed_canonical_item(conn, "APP-KEEP-001", "Keep", src)
    _seed_canonical_item(conn, "APP-DEL-001", "WillBeRemoved", src)
    _seed_canonical_item(conn, "APP-DEL-002", "AlsoRemoved", src)

    batch_id = _upload_and_validate(
        staging_client, auth, conn,
        [("APP-KEEP-001", "Keep", "component", "EA", "active")],
        source_system=src,
    )

    resp = staging_client.post(
        f"/v1/staging/batches/{batch_id}/approve",
        headers=auth,
        json={"approved_by": "test@example.com"},
    )
    body = resp.json()
    assert body["counts"]["rows_soft_deleted"] == 2

    # Canonical items got status='obsolete'
    obsolete = conn.execute(
        "SELECT external_id, status FROM items "
        "WHERE external_id IN ('APP-DEL-001', 'APP-DEL-002') ORDER BY external_id"
    ).fetchall()
    assert all(r["status"] == "obsolete" for r in obsolete)

    # external_references entries removed for this source
    refs = conn.execute(
        "SELECT external_id FROM external_references "
        "WHERE source_system = %s AND external_id LIKE 'APP-DEL-%'",
        (src,),
    ).fetchall()
    assert len(refs) == 0


# ---------------------------------------------------------------------------
# 20% deletion ratio guard
# ---------------------------------------------------------------------------


@requires_db
def test_approve_blocks_excessive_deletion_without_force(
    staging_client, auth, conn,
) -> None:
    """5 canonical, batch keeps 2 -> 60% deletion. Refuses without force."""
    src = "APPROVE-GRD-" + uuid4().hex[:6]
    for i in range(5):
        _seed_canonical_item(conn, f"APP-GRD-{i:03d}", f"Row{i}", src)

    batch_id = _upload_and_validate(
        staging_client, auth, conn,
        [
            ("APP-GRD-000", "Row0", "component", "EA", "active"),
            ("APP-GRD-001", "Row1", "component", "EA", "active"),
        ],
        source_system=src,
    )

    # Without force=true: 400
    resp = staging_client.post(
        f"/v1/staging/batches/{batch_id}/approve",
        headers=auth,
        json={"approved_by": "test@example.com"},
    )
    assert resp.status_code == 400
    assert "deletion ratio" in resp.json()["detail"]
    # Batch must STAY validated — nothing got written
    batch = conn.execute(
        "SELECT status FROM ingest_batches WHERE batch_id = %s",
        (batch_id,),
    ).fetchone()
    assert batch["status"] == "validated"


@requires_db
def test_approve_proceeds_with_force_true(staging_client, auth, conn) -> None:
    """Same setup, but force=true -> approval succeeds, audit flag set."""
    src = "APPROVE-GRD2-" + uuid4().hex[:6]
    for i in range(5):
        _seed_canonical_item(conn, f"APP-FRC-{i:03d}", f"Row{i}", src)

    batch_id = _upload_and_validate(
        staging_client, auth, conn,
        [
            ("APP-FRC-000", "Row0", "component", "EA", "active"),
            ("APP-FRC-001", "Row1", "component", "EA", "active"),
        ],
        source_system=src,
    )

    resp = staging_client.post(
        f"/v1/staging/batches/{batch_id}/approve",
        headers=auth,
        json={
            "approved_by": "test@example.com",
            "notes": "Intentional scope reduction — confirmed with planning",
            "force": True,
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["forced_approval"] is True
    assert body["counts"]["rows_soft_deleted"] == 3


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------


@requires_db
def test_approve_rejects_pending_batch(staging_client, auth, conn) -> None:
    """A batch still in 'pending' (no DQ yet) can't be approved."""
    src = "APPROVE-PEND-" + uuid4().hex[:6]
    # Upload but DO NOT mark as validated
    lines = ["external_id\tname\titem_type\tuom\tstatus",
             "APP-PEND-001\tFoo\tcomponent\tEA\tactive"]
    data = ("\n".join(lines) + "\n").encode("utf-8")
    resp = staging_client.post(
        "/v1/staging/upload",
        headers=auth,
        files={"file": ("items.tsv", io.BytesIO(data), "text/plain")},
        data={"entity_type": "items", "source_system": src},
    )
    batch_id = resp.json()["batch_id"]

    resp = staging_client.post(
        f"/v1/staging/batches/{batch_id}/approve",
        headers=auth,
        json={"approved_by": "test@example.com"},
    )
    assert resp.status_code == 400
    assert "validated" in resp.json()["detail"]


@requires_db
def test_approve_rejects_unknown_batch(staging_client, auth) -> None:
    bogus = uuid4()
    resp = staging_client.post(
        f"/v1/staging/batches/{bogus}/approve",
        headers=auth,
        json={"approved_by": "test@example.com"},
    )
    assert resp.status_code == 400
    assert "not found" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# transform_runs audit
# ---------------------------------------------------------------------------


@requires_db
def test_approve_writes_transform_runs_audit(staging_client, auth, conn) -> None:
    """staging.transform_runs must hold a 'completed' row with the
    counters + approver after a successful approval."""
    src = "APPROVE-AUD-" + uuid4().hex[:6]
    batch_id = _upload_and_validate(
        staging_client, auth, conn,
        [("APP-AUD-001", "Audited", "component", "EA", "active")],
        source_system=src,
    )
    body = staging_client.post(
        f"/v1/staging/batches/{batch_id}/approve",
        headers=auth,
        json={"approved_by": "auditor@example.com", "notes": "for the record"},
    ).json()

    run_row = conn.execute(
        "SELECT * FROM staging.transform_runs WHERE run_id = %s",
        (body["run_id"],),
    ).fetchone()
    assert run_row is not None
    assert run_row["status"] == "completed"
    assert run_row["approved_by"] == "auditor@example.com"
    assert run_row["approval_notes"] == "for the record"
    assert run_row["rows_inserted"] == 1
    assert run_row["rows_soft_deleted"] == 0
    assert run_row["completed_at"] is not None
