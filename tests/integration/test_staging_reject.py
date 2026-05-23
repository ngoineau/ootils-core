"""Integration tests for POST /v1/staging/batches/{id}/reject.

Closes out a batch as 'rejected' (terminal), records the rejection
in staging.transform_runs + ingest_batches.notes for audit.
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


def _upload_basic_batch(staging_client, auth, source_system="REJ-TEST") -> UUID:
    """Upload a tiny valid TSV; batch lands in 'pending'."""
    data = b"external_id\tname\titem_type\tuom\tstatus\nREJ-001\tFoo\tcomponent\tEA\tactive\n"
    resp = staging_client.post(
        "/v1/staging/upload",
        headers=auth,
        files={"file": ("items.tsv", io.BytesIO(data), "text/plain")},
        data={"entity_type": "items", "source_system": source_system},
    )
    assert resp.status_code == 202, resp.text
    return UUID(resp.json()["batch_id"])


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------


@requires_db
def test_reject_pending_batch(staging_client, auth, conn) -> None:
    batch_id = _upload_basic_batch(staging_client, auth, source_system="REJ-PEND")

    resp = staging_client.post(
        f"/v1/staging/batches/{batch_id}/reject",
        headers=auth,
        json={"rejected_by": "ops@example.com",
              "reason": "wrong source_system spelling"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["new_status"] == "rejected"
    assert body["prior_status"] == "pending"
    assert body["rejected_by"] == "ops@example.com"
    assert body["rejection_reason"] == "wrong source_system spelling"

    # Batch transitioned to rejected; reason appended to notes
    batch = conn.execute(
        "SELECT status, notes FROM ingest_batches WHERE batch_id = %s",
        (batch_id,),
    ).fetchone()
    assert batch["status"] == "rejected"
    assert "wrong source_system spelling" in batch["notes"]
    assert "ops@example.com" in batch["notes"]

    # transform_runs got a rolled_back audit row
    run = conn.execute(
        "SELECT * FROM staging.transform_runs WHERE run_id = %s",
        (body["run_id"],),
    ).fetchone()
    assert run["status"] == "rolled_back"
    assert run["approved_by"] == "ops@example.com"
    assert run["approval_notes"] == "wrong source_system spelling"


@requires_db
def test_reject_validated_batch(staging_client, auth, conn) -> None:
    """A batch already marked 'validated' (DQ-clean) can still be rejected
    — e.g. operator sees in /diff that the batch would soft-delete too
    much, and refuses to approve."""
    batch_id = _upload_basic_batch(staging_client, auth, source_system="REJ-VAL")
    conn.execute(
        "UPDATE ingest_batches SET status = 'validated' WHERE batch_id = %s",
        (batch_id,),
    )
    conn.commit()

    resp = staging_client.post(
        f"/v1/staging/batches/{batch_id}/reject",
        headers=auth,
        json={"rejected_by": "ops@example.com",
              "reason": "/diff showed 60% deletion ratio — not approving"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["prior_status"] == "validated"
    assert body["new_status"] == "rejected"


# ---------------------------------------------------------------------------
# ingest_rows preserved (rejected batch keeps its raw rows for forensics)
# ---------------------------------------------------------------------------


@requires_db
def test_reject_preserves_ingest_rows(staging_client, auth, conn) -> None:
    batch_id = _upload_basic_batch(staging_client, auth, source_system="REJ-KEEP")
    rows_before = conn.execute(
        "SELECT COUNT(*) AS n FROM ingest_rows WHERE batch_id = %s",
        (batch_id,),
    ).fetchone()
    assert rows_before["n"] == 1

    staging_client.post(
        f"/v1/staging/batches/{batch_id}/reject",
        headers=auth,
        json={"rejected_by": "x@y", "reason": "keep me"},
    )

    rows_after = conn.execute(
        "SELECT COUNT(*) AS n FROM ingest_rows WHERE batch_id = %s",
        (batch_id,),
    ).fetchone()
    assert rows_after["n"] == 1  # raw rows stay for audit


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------


@requires_db
def test_reject_unknown_batch_returns_400(staging_client, auth) -> None:
    bogus = uuid4()
    resp = staging_client.post(
        f"/v1/staging/batches/{bogus}/reject",
        headers=auth,
        json={"rejected_by": "x@y", "reason": "x"},
    )
    assert resp.status_code == 400
    assert "not found" in resp.json()["detail"]


@requires_db
def test_reject_already_rejected_refused(staging_client, auth, conn) -> None:
    batch_id = _upload_basic_batch(staging_client, auth, source_system="REJ-TWICE")
    staging_client.post(
        f"/v1/staging/batches/{batch_id}/reject",
        headers=auth,
        json={"rejected_by": "x@y", "reason": "first"},
    )

    # Second rejection attempt -> 400
    resp = staging_client.post(
        f"/v1/staging/batches/{batch_id}/reject",
        headers=auth,
        json={"rejected_by": "x@y", "reason": "second"},
    )
    assert resp.status_code == 400
    assert "terminal status" in resp.json()["detail"]


@requires_db
def test_reject_imported_batch_refused(staging_client, auth, conn) -> None:
    """A batch already imported into canonical can't be rejected — that
    ship has sailed; you'd need a compensating batch instead."""
    batch_id = uuid4()
    conn.execute(
        """
        INSERT INTO ingest_batches
            (batch_id, entity_type, source_system, status, total_rows)
        VALUES (%s, 'items', 'X', 'imported', 0)
        """,
        (batch_id,),
    )
    conn.commit()

    resp = staging_client.post(
        f"/v1/staging/batches/{batch_id}/reject",
        headers=auth,
        json={"rejected_by": "x@y", "reason": "too late"},
    )
    assert resp.status_code == 400
    assert "terminal status" in resp.json()["detail"]


@requires_db
def test_reject_requires_non_empty_reason(staging_client, auth, conn) -> None:
    """Pydantic's min_length=1 enforces this at the body schema level
    (returns 422), backstopped by RejectionError if the body validation
    is bypassed somehow."""
    batch_id = _upload_basic_batch(staging_client, auth, source_system="REJ-NOREAS")

    resp = staging_client.post(
        f"/v1/staging/batches/{batch_id}/reject",
        headers=auth,
        json={"rejected_by": "x@y", "reason": ""},
    )
    assert resp.status_code == 422  # FastAPI/pydantic validation


@requires_db
def test_reject_blank_reason_refused(staging_client, auth, conn) -> None:
    """A reason containing only whitespace passes pydantic length but
    fails our explicit strip-check inside RejectionError."""
    batch_id = _upload_basic_batch(staging_client, auth, source_system="REJ-BLANK")
    resp = staging_client.post(
        f"/v1/staging/batches/{batch_id}/reject",
        headers=auth,
        json={"rejected_by": "x@y", "reason": "    "},
    )
    assert resp.status_code == 400
    assert "reason" in resp.json()["detail"].lower()
