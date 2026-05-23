"""Integration tests for POST /v1/staging/upload (ADR-013 step 4).

Builds on the test patterns in test_ingest.py — uses a TestClient
wired to the migrated test DB, with auth via OOTILS_API_TOKEN.
"""
from __future__ import annotations

import io
import json
import os

import psycopg
import pytest
from psycopg.rows import dict_row

from .conftest import requires_db


@pytest.fixture(scope="module")
def staging_client(migrated_db):
    """Module-scoped TestClient for /v1/staging/* endpoints."""
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


@pytest.fixture(scope="module")
def auth():
    return {"Authorization": "Bearer test-token"}


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


@requires_db
def test_upload_tsv_items(staging_client, auth, migrated_db: str) -> None:
    """Upload a tiny TSV items file, verify 202 + DB state."""
    data = (
        b"external_id\tname\titem_type\tuom\tstatus\n"
        b"SAP-001\tWidget A\tfinished_good\tEA\tactive\n"
        b"SAP-002\tWidget B\tcomponent\tEA\tphase_out\n"
    )
    resp = staging_client.post(
        "/v1/staging/upload",
        headers=auth,
        files={"file": ("items.tsv", io.BytesIO(data), "text/tab-separated-values")},
        data={
            "entity_type": "items",
            "source_system": "SAP-EU",
            "notes": "weekly refresh",
        },
    )
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["status"] == "pending"
    assert body["entity_type"] == "items"
    assert body["source_system"] == "SAP-EU"
    assert body["rows_inserted"] == 2
    assert body["format"] == "tsv"
    assert body["delimiter"] == "\t"
    assert body["encoding"] == "utf-8"
    assert body["headers"] == ["external_id", "name", "item_type", "uom", "status"]
    assert len(body["sha256"]) == 64
    assert body["file_size_bytes"] == len(data)

    # Verify staging.uploads + ingest_batches were written
    with psycopg.connect(migrated_db, row_factory=dict_row) as c:
        up = c.execute(
            "SELECT * FROM staging.uploads WHERE upload_id = %s",
            (body["upload_id"],),
        ).fetchone()
        assert up is not None
        assert str(up["batch_id"]) == body["batch_id"]
        assert up["sha256"] == body["sha256"]
        assert up["filename"] == "items.tsv"

        batch = c.execute(
            "SELECT * FROM ingest_batches WHERE batch_id = %s",
            (body["batch_id"],),
        ).fetchone()
        assert batch["status"] == "pending"
        assert batch["entity_type"] == "items"
        assert batch["total_rows"] == 2

        rows = c.execute(
            "SELECT row_number, col_01, col_02 FROM ingest_rows "
            "WHERE batch_id = %s ORDER BY row_number",
            (body["batch_id"],),
        ).fetchall()
        assert len(rows) == 2
        assert rows[0]["col_01"] == "SAP-001"
        assert rows[1]["col_02"] == "Widget B"


@requires_db
def test_upload_csv_with_semicolon_sniffed(staging_client, auth) -> None:
    data = b"external_id;name\nA1;Foo\nA2;Bar\n"
    resp = staging_client.post(
        "/v1/staging/upload",
        headers=auth,
        files={"file": ("items.csv", io.BytesIO(data), "text/csv")},
        data={"entity_type": "items", "source_system": "OPS-EU"},
    )
    assert resp.status_code == 202, resp.text
    assert resp.json()["delimiter"] == ";"
    assert resp.json()["format"] == "csv"


@requires_db
def test_upload_json(staging_client, auth) -> None:
    payload = json.dumps([
        {"external_id": "J1", "name": "Foo", "item_type": "component", "uom": "EA"},
        {"external_id": "J2", "name": "Bar", "item_type": "component", "uom": "EA"},
    ]).encode("utf-8")
    resp = staging_client.post(
        "/v1/staging/upload",
        headers=auth,
        files={"file": ("items.json", io.BytesIO(payload), "application/json")},
        data={"entity_type": "items", "source_system": "API-PUSH"},
    )
    assert resp.status_code == 202, resp.text
    assert resp.json()["format"] == "json"
    assert resp.json()["rows_inserted"] == 2


# ---------------------------------------------------------------------------
# Validation errors
# ---------------------------------------------------------------------------


@requires_db
def test_upload_rejects_unknown_entity_type(staging_client, auth) -> None:
    resp = staging_client.post(
        "/v1/staging/upload",
        headers=auth,
        files={"file": ("x.tsv", io.BytesIO(b"a\tb\n1\t2\n"), "text/plain")},
        data={"entity_type": "nonsense", "source_system": "X"},
    )
    assert resp.status_code == 400
    assert "unknown entity_type" in resp.json()["detail"]


@requires_db
def test_upload_rejects_empty_file(staging_client, auth) -> None:
    resp = staging_client.post(
        "/v1/staging/upload",
        headers=auth,
        files={"file": ("x.tsv", io.BytesIO(b""), "text/plain")},
        data={"entity_type": "items", "source_system": "X"},
    )
    assert resp.status_code == 400
    assert "empty" in resp.json()["detail"].lower()


@requires_db
def test_upload_rejects_missing_source_system(staging_client, auth) -> None:
    resp = staging_client.post(
        "/v1/staging/upload",
        headers=auth,
        files={"file": ("x.tsv", io.BytesIO(b"a\tb\n1\t2\n"), "text/plain")},
        data={"entity_type": "items", "source_system": "  "},
    )
    assert resp.status_code == 400
    assert "source_system" in resp.json()["detail"]


@requires_db
def test_upload_rejects_malformed_json(staging_client, auth) -> None:
    """A .json file that can't be parsed surfaces as a 400 parse error."""
    resp = staging_client.post(
        "/v1/staging/upload",
        headers=auth,
        files={"file": ("bad.json", io.BytesIO(b"{not json"), "application/json")},
        data={"entity_type": "items", "source_system": "X"},
    )
    assert resp.status_code == 400
    assert "parse error" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------


@requires_db
def test_upload_requires_auth(staging_client) -> None:
    resp = staging_client.post(
        "/v1/staging/upload",
        files={"file": ("x.tsv", io.BytesIO(b"a\tb\n1\t2\n"), "text/plain")},
        data={"entity_type": "items", "source_system": "X"},
    )
    # Either 401 or 403 depending on the auth dependency's behaviour
    assert resp.status_code in (401, 403)
