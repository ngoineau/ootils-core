"""Integration tests for staging.loader — round-trip from ParseResult into Postgres.

Requires a real Postgres with migrations 001..033 applied. Uses the
existing `migrated_db` fixture in tests/integration/conftest.py.
"""
from __future__ import annotations

import json

import psycopg
import pytest
from psycopg.rows import dict_row

from ootils_core.staging.loader import (
    MAX_COLUMNS_PER_ROW,
    LoaderError,
    load_to_staging,
)
from ootils_core.staging.parser import parse

from .conftest import requires_db


@pytest.fixture
def conn(migrated_db: str):
    """A clean connection per test, rolled back at teardown to avoid
    polluting other tests."""
    with psycopg.connect(migrated_db, row_factory=dict_row) as c:
        yield c
        c.rollback()


@requires_db
def test_loader_inserts_upload_batch_and_rows(conn) -> None:
    """End-to-end: parse TSV bytes, load to staging, verify all three tables."""
    data = (
        b"external_id\tname\titem_type\tuom\tstatus\n"
        b"SAP-001\tWidget A\tfinished_good\tEA\tactive\n"
        b"SAP-002\tWidget B\tcomponent\tEA\tphase_out\n"
        b"SAP-003\tWidget C\traw_material\tKG\tactive\n"
    )
    pr = parse(data, filename="items_sap.tsv")
    result = load_to_staging(
        conn,
        parse_result=pr,
        entity_type="items",
        source_system="SAP-EU",
        raw_bytes_size=len(data),
        filename="items_sap.tsv",
        content_type="text/tab-separated-values",
        submitted_by="ops@example.com",
        notes="weekly SAP master refresh",
    )

    assert result.rows_inserted == 3
    assert result.format == "tsv"
    assert result.encoding == "utf-8"
    assert result.headers == ["external_id", "name", "item_type", "uom", "status"]

    # staging.uploads — exactly 1 row, linked to the batch
    upload = conn.execute(
        "SELECT * FROM staging.uploads WHERE upload_id = %s", (result.upload_id,)
    ).fetchone()
    assert upload is not None
    assert upload["batch_id"] == result.batch_id
    assert upload["filename"] == "items_sap.tsv"
    assert upload["file_format"] == "tsv"
    assert upload["file_size_bytes"] == len(data)
    assert upload["sha256"] == pr.sha256
    assert upload["uploaded_by"] == "ops@example.com"

    # ingest_batches — exactly 1 row, status=pending, count=3
    batch = conn.execute(
        "SELECT * FROM ingest_batches WHERE batch_id = %s", (result.batch_id,)
    ).fetchone()
    assert batch is not None
    assert batch["entity_type"] == "items"
    assert batch["source_system"] == "SAP-EU"
    assert batch["status"] == "pending"
    assert batch["total_rows"] == 3
    assert batch["submitted_by"] == "ops@example.com"
    assert batch["notes"] == "weekly SAP master refresh"

    # ingest_rows — 3 rows, row_number 1..3, col_01..col_05 populated
    rows = conn.execute(
        """
        SELECT row_number, raw_content, col_01, col_02, col_03, col_04, col_05, col_06
        FROM ingest_rows WHERE batch_id = %s ORDER BY row_number
        """,
        (result.batch_id,),
    ).fetchall()
    assert len(rows) == 3
    assert [r["row_number"] for r in rows] == [1, 2, 3]
    assert rows[0]["col_01"] == "SAP-001"
    assert rows[0]["col_02"] == "Widget A"
    assert rows[0]["col_03"] == "finished_good"
    assert rows[0]["col_04"] == "EA"
    assert rows[0]["col_05"] == "active"
    # No 6th column in the file -> col_06 stays NULL
    assert rows[0]["col_06"] is None
    # raw_content holds the JSON dict so DQ can look up by header name
    parsed = json.loads(rows[0]["raw_content"])
    assert parsed["external_id"] == "SAP-001"
    assert parsed["status"] == "active"


@requires_db
def test_loader_too_many_columns_raises(conn) -> None:
    headers = "\t".join(f"col{i}" for i in range(MAX_COLUMNS_PER_ROW + 1))
    values = "\t".join(f"v{i}" for i in range(MAX_COLUMNS_PER_ROW + 1))
    data = f"{headers}\n{values}\n".encode("utf-8")
    pr = parse(data, format_hint="tsv")
    with pytest.raises(LoaderError, match=str(MAX_COLUMNS_PER_ROW)):
        load_to_staging(
            conn,
            parse_result=pr,
            entity_type="items",
            source_system="SAP-EU",
            raw_bytes_size=len(data),
            filename="too_wide.tsv",
        )


@requires_db
def test_loader_empty_rows_still_creates_batch(conn) -> None:
    """A header-only file should still produce a batch + upload row,
    just with total_rows=0. Useful for catching 'export ran but emitted
    nothing' situations."""
    data = b"external_id\tname\n"
    pr = parse(data, format_hint="tsv")
    result = load_to_staging(
        conn,
        parse_result=pr,
        entity_type="items",
        source_system="SAP-EU",
        raw_bytes_size=len(data),
        filename="empty_items.tsv",
    )
    assert result.rows_inserted == 0
    batch = conn.execute(
        "SELECT total_rows FROM ingest_batches WHERE batch_id = %s",
        (result.batch_id,),
    ).fetchone()
    assert batch["total_rows"] == 0
    # ingest_rows table empty for this batch
    cnt = conn.execute(
        "SELECT COUNT(*) AS n FROM ingest_rows WHERE batch_id = %s",
        (result.batch_id,),
    ).fetchone()["n"]
    assert cnt == 0


@requires_db
def test_loader_preserves_non_ascii_values(conn) -> None:
    """Round-trip with French + accents to confirm UTF-8 survives the
    JSON serialisation in raw_content."""
    data = (
        b"external_id\tname\n"
        + "RM-001\tAcier inoxydable français\n".encode("utf-8")
        + "RM-002\tÉpoxy résiné\n".encode("utf-8")
    )
    pr = parse(data, format_hint="tsv")
    result = load_to_staging(
        conn,
        parse_result=pr,
        entity_type="items",
        source_system="SAP-FR",
        raw_bytes_size=len(data),
        filename="raws_fr.tsv",
    )
    rows = conn.execute(
        "SELECT raw_content, col_02 FROM ingest_rows WHERE batch_id = %s ORDER BY row_number",
        (result.batch_id,),
    ).fetchall()
    assert rows[0]["col_02"] == "Acier inoxydable français"
    parsed = json.loads(rows[0]["raw_content"])
    assert parsed["name"] == "Acier inoxydable français"


@requires_db
def test_loader_bulk_insert_handles_500_rows(conn) -> None:
    """Sanity check on bulk insert path — make sure the UNNEST with 18
    parallel arrays survives a non-trivial row count."""
    lines = [b"external_id\tname\titem_type\tuom\tstatus"]
    for i in range(500):
        lines.append(f"BULK-{i:04d}\tBulk item {i}\tcomponent\tEA\tactive".encode("utf-8"))
    data = b"\n".join(lines) + b"\n"
    pr = parse(data, format_hint="tsv")
    result = load_to_staging(
        conn,
        parse_result=pr,
        entity_type="items",
        source_system="BULK-TEST",
        raw_bytes_size=len(data),
        filename="bulk.tsv",
    )
    assert result.rows_inserted == 500
    cnt = conn.execute(
        "SELECT COUNT(*) AS n FROM ingest_rows WHERE batch_id = %s",
        (result.batch_id,),
    ).fetchone()["n"]
    assert cnt == 500
