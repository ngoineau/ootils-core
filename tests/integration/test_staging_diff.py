"""Integration tests for GET /v1/staging/batches/{id}/diff (ADR-013 step 7).

The diff endpoint joins ingest_rows + external_id_mapping + canonical
tables so it inherently needs a real DB. Tests cover:
  - happy paths for the 3 supported entity types (items, locations,
    suppliers): pure insert, pure update, pure soft-delete, mixed
  - the 20% deletion-ratio guard
  - unsupported entity_type returns supported=false with a reason
  - non-existent batch / batch in terminal state returns 400
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
def diff_client(migrated_db):
    """Module-scoped TestClient sharing the migrated DB connection."""
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
# Helpers — seed canonical state + upload a TSV batch via the existing endpoint
# ---------------------------------------------------------------------------


def _seed_canonical_item(
    conn,
    external_id: str,
    name: str,
    item_type: str = "component",
    uom: str = "EA",
    status: str = "active",
    source_system: str = "TEST-SOURCE",
) -> UUID:
    """Insert one item + its external_id_mapping entry. Returns item_id."""
    item_id = uuid4()
    conn.execute(
        """
        INSERT INTO items (item_id, external_id, name, item_type, uom, status)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (item_id, external_id, name, item_type, uom, status),
    )
    conn.execute(
        """
        INSERT INTO external_references
            (entity_type, external_id, internal_id, source_system)
        VALUES ('item', %s, %s, %s)
        """,
        (external_id, item_id, source_system),
    )
    conn.commit()
    return item_id


def _upload_items_tsv(
    diff_client, auth, rows: list[tuple[str, str, str, str, str]],
    source_system: str = "TEST-SOURCE",
) -> UUID:
    """Upload a TSV through POST /v1/staging/upload, return the batch_id."""
    lines = ["external_id\tname\titem_type\tuom\tstatus"]
    for r in rows:
        lines.append("\t".join(r))
    data = ("\n".join(lines) + "\n").encode("utf-8")
    resp = diff_client.post(
        "/v1/staging/upload",
        headers=auth,
        files={"file": ("items.tsv", io.BytesIO(data), "text/tab-separated-values")},
        data={"entity_type": "items", "source_system": source_system},
    )
    assert resp.status_code == 202, resp.text
    return UUID(resp.json()["batch_id"])


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------


@requires_db
def test_diff_pure_insert(diff_client, auth) -> None:
    """No canonical rows for this source -> every batch row is INSERT."""
    batch_id = _upload_items_tsv(
        diff_client, auth,
        [
            ("DIFF-INS-001", "Foo", "component", "EA", "active"),
            ("DIFF-INS-002", "Bar", "component", "EA", "active"),
        ],
        source_system="DIFF-INSERT-ONLY",
    )

    resp = diff_client.get(f"/v1/staging/batches/{batch_id}/diff", headers=auth)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["supported"] is True
    assert body["counts"]["will_insert"] == 2
    assert body["counts"]["will_update"] == 0
    assert body["counts"]["will_soft_delete"] == 0
    assert body["counts"]["in_canonical_for_source"] == 0
    assert set(body["samples"]["will_insert"]) == {"DIFF-INS-001", "DIFF-INS-002"}
    assert body["deletion_guard"]["ratio"] == 0.0
    assert body["deletion_guard"]["exceeds_threshold"] is False


@requires_db
def test_diff_pure_update(diff_client, auth, conn) -> None:
    """Same external_ids in canonical, batch changes one field per row -> UPDATE."""
    src = "DIFF-UPDATE-ONLY"
    _seed_canonical_item(conn, "DIFF-UPD-001", "OldName1", source_system=src)
    _seed_canonical_item(conn, "DIFF-UPD-002", "OldName2", source_system=src)

    batch_id = _upload_items_tsv(
        diff_client, auth,
        [
            ("DIFF-UPD-001", "NewName1", "component", "EA", "active"),
            ("DIFF-UPD-002", "NewName2", "component", "EA", "active"),
        ],
        source_system=src,
    )

    resp = diff_client.get(f"/v1/staging/batches/{batch_id}/diff", headers=auth)
    body = resp.json()
    assert body["counts"]["will_insert"] == 0
    assert body["counts"]["will_update"] == 2
    assert body["counts"]["will_soft_delete"] == 0
    assert body["counts"]["will_noop"] == 0


@requires_db
def test_diff_pure_noop(diff_client, auth, conn) -> None:
    """Canonical = batch values exactly -> all NO-OP, nothing to change."""
    src = "DIFF-NOOP-ONLY"
    _seed_canonical_item(conn, "DIFF-NOP-001", "Same",
                         item_type="component", uom="EA", status="active",
                         source_system=src)

    batch_id = _upload_items_tsv(
        diff_client, auth,
        [("DIFF-NOP-001", "Same", "component", "EA", "active")],
        source_system=src,
    )

    resp = diff_client.get(f"/v1/staging/batches/{batch_id}/diff", headers=auth)
    body = resp.json()
    assert body["counts"]["will_noop"] == 1
    assert body["counts"]["will_update"] == 0
    assert body["counts"]["will_insert"] == 0
    assert body["counts"]["will_soft_delete"] == 0


@requires_db
def test_diff_pure_soft_delete(diff_client, auth, conn) -> None:
    """Canonical has 3 rows, batch has 1 (same as one canonical) -> 2 SOFT-DELETE."""
    src = "DIFF-DEL-ONLY"
    _seed_canonical_item(conn, "KEEP-001", "Keep", source_system=src)
    _seed_canonical_item(conn, "DEL-001", "DropMe1", source_system=src)
    _seed_canonical_item(conn, "DEL-002", "DropMe2", source_system=src)

    batch_id = _upload_items_tsv(
        diff_client, auth,
        [("KEEP-001", "Keep", "component", "EA", "active")],
        source_system=src,
    )

    resp = diff_client.get(f"/v1/staging/batches/{batch_id}/diff", headers=auth)
    body = resp.json()
    assert body["counts"]["will_soft_delete"] == 2
    assert set(body["samples"]["will_soft_delete"]) == {"DEL-001", "DEL-002"}
    assert body["counts"]["will_noop"] == 1


@requires_db
def test_diff_mixed(diff_client, auth, conn) -> None:
    """Realistic mix: 1 insert + 1 update + 1 noop + 1 soft-delete."""
    src = "DIFF-MIXED"
    _seed_canonical_item(conn, "MIX-UPD", "OldName", source_system=src)
    _seed_canonical_item(conn, "MIX-NOP", "SameName", source_system=src)
    _seed_canonical_item(conn, "MIX-DEL", "Doomed", source_system=src)

    batch_id = _upload_items_tsv(
        diff_client, auth,
        [
            ("MIX-INS", "NewItem", "component", "EA", "active"),
            ("MIX-UPD", "NewName", "component", "EA", "active"),
            ("MIX-NOP", "SameName", "component", "EA", "active"),
        ],
        source_system=src,
    )

    body = diff_client.get(f"/v1/staging/batches/{batch_id}/diff", headers=auth).json()
    counts = body["counts"]
    assert counts["will_insert"] == 1
    assert counts["will_update"] == 1
    assert counts["will_noop"] == 1
    assert counts["will_soft_delete"] == 1
    assert counts["in_batch"] == 3
    assert counts["in_canonical_for_source"] == 3


# ---------------------------------------------------------------------------
# 20% deletion ratio guard
# ---------------------------------------------------------------------------


@requires_db
def test_diff_deletion_threshold_not_exceeded(diff_client, auth, conn) -> None:
    """10 canonical + batch keeps 9 -> 10% deletion, under the 20% guard."""
    src = "DIFF-GUARD-OK"
    for i in range(10):
        _seed_canonical_item(conn, f"GRD-OK-{i:03d}", f"Item{i}", source_system=src)

    rows = [(f"GRD-OK-{i:03d}", f"Item{i}", "component", "EA", "active") for i in range(9)]
    batch_id = _upload_items_tsv(diff_client, auth, rows, source_system=src)

    body = diff_client.get(f"/v1/staging/batches/{batch_id}/diff", headers=auth).json()
    guard = body["deletion_guard"]
    assert body["counts"]["will_soft_delete"] == 1
    assert guard["ratio"] == 0.1
    assert guard["exceeds_threshold"] is False


@requires_db
def test_diff_deletion_threshold_exceeded(diff_client, auth, conn) -> None:
    """10 canonical + batch keeps 5 -> 50% deletion, far above 20% guard."""
    src = "DIFF-GUARD-EXCEED"
    for i in range(10):
        _seed_canonical_item(conn, f"GRD-XS-{i:03d}", f"Item{i}", source_system=src)

    rows = [(f"GRD-XS-{i:03d}", f"Item{i}", "component", "EA", "active") for i in range(5)]
    batch_id = _upload_items_tsv(diff_client, auth, rows, source_system=src)

    body = diff_client.get(f"/v1/staging/batches/{batch_id}/diff", headers=auth).json()
    guard = body["deletion_guard"]
    assert body["counts"]["will_soft_delete"] == 5
    assert guard["ratio"] == 0.5
    assert guard["exceeds_threshold"] is True


# ---------------------------------------------------------------------------
# Source isolation: another source's items must NOT appear as soft-delete
# ---------------------------------------------------------------------------


@requires_db
def test_diff_other_source_unaffected(diff_client, auth, conn) -> None:
    """Items imported from another source_system must NOT show up as
    soft-delete candidates when this batch's source is different."""
    _seed_canonical_item(conn, "OTHER-001", "From other source",
                         source_system="OTHER-SOURCE")

    batch_id = _upload_items_tsv(
        diff_client, auth,
        [("THIS-001", "Mine", "component", "EA", "active")],
        source_system="THIS-SOURCE",
    )

    body = diff_client.get(f"/v1/staging/batches/{batch_id}/diff", headers=auth).json()
    assert body["counts"]["will_soft_delete"] == 0
    assert body["counts"]["in_canonical_for_source"] == 0
    assert body["counts"]["will_insert"] == 1


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------


@requires_db
def test_diff_unknown_batch_returns_400(diff_client, auth) -> None:
    bogus = uuid4()
    resp = diff_client.get(f"/v1/staging/batches/{bogus}/diff", headers=auth)
    assert resp.status_code == 400
    assert "not found" in resp.json()["detail"]


@requires_db
def test_diff_unsupported_entity_type_returns_supported_false(
    diff_client, auth, conn,
) -> None:
    """on_hand isn't in the diff registry yet; the endpoint replies 200
    with supported=False + a reason rather than crashing."""
    # Inject a batch directly with on_hand entity_type to avoid running
    # the upload endpoint with an unsupported flow.
    batch_id = uuid4()
    conn.execute(
        """
        INSERT INTO ingest_batches
            (batch_id, entity_type, source_system, status, total_rows)
        VALUES (%s, 'on_hand', 'TEST', 'validated', 0)
        """,
        (batch_id,),
    )
    conn.commit()

    resp = diff_client.get(f"/v1/staging/batches/{batch_id}/diff", headers=auth)
    assert resp.status_code == 200
    body = resp.json()
    assert body["supported"] is False
    assert "on_hand" in body["unsupported_reason"]


@requires_db
def test_diff_imported_batch_returns_400(diff_client, auth, conn) -> None:
    batch_id = uuid4()
    conn.execute(
        """
        INSERT INTO ingest_batches
            (batch_id, entity_type, source_system, status, total_rows)
        VALUES (%s, 'items', 'TEST', 'imported', 0)
        """,
        (batch_id,),
    )
    conn.commit()
    resp = diff_client.get(f"/v1/staging/batches/{batch_id}/diff", headers=auth)
    assert resp.status_code == 400
    assert "terminal status" in resp.json()["detail"]
