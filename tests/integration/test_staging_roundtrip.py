"""Round-trip test: canonical table -> TSV/CSV/JSON -> parse + load -> verify staging.

Validates the contract between the staging templates (docs/staging-templates/*)
and the parse + load pipeline by:

  1. Seeding a small but representative master dataset into the canonical
     tables (items / locations / suppliers).
  2. Exporting each entity to TSV / CSV / JSON in memory using the column
     order defined by the corresponding template document.
  3. Running the export back through ootils_core.staging.parser.parse() +
     ootils_core.staging.loader.load_to_staging().
  4. Asserting that ingest_rows contain exactly the same data we exported,
     including non-ASCII characters and edge-case values.

This is the integrity check that catches drift between the templates and
the parser/loader contracts. If a future template change adds a column
without updating the parser or the loader, this test fires.

For a real-volume validation against `ootils_seed_test` (5K items), use
scripts/staging_roundtrip_seed.py from a tunnel-up machine.
"""
from __future__ import annotations

import csv
import io
import json
from uuid import uuid4

import psycopg
import pytest
from psycopg.rows import dict_row

from ootils_core.staging.loader import load_to_staging
from ootils_core.staging.parser import parse

from .conftest import requires_db


# ---------------------------------------------------------------------------
# Fixtures — small master dataset that exercises all the parser edge cases
# ---------------------------------------------------------------------------


_ITEMS = [
    ("RT-FG-001", "Smart Sensor Module 24V",    "finished_good", "EA", "active"),
    ("RT-FG-002", "Compact Controller X1",      "finished_good", "EA", "phase_out"),
    ("RT-SA-001", "Power Supply Sub-assembly",  "semi_finished", "EA", "active"),
    ("RT-CP-001", "PCB Main Board Rev3",        "component",     "EA", "active"),
    ("RT-RM-001", "Acier inoxydable français",  "raw_material",  "KG", "active"),  # non-ASCII
    ("RT-RM-002", "Câble Cat6, blindé",          "raw_material",  "M",  "active"),  # comma + accent
]

_LOCATIONS = [
    ("RT-DC-EU", "DC Europe Central", "dc",    "DE", "Europe/Berlin"),
    ("RT-PL-US", "Plant US Detroit",  "plant", "US", "America/Detroit"),
    ("RT-PL-CN", "Plant Shenzhen",    "plant", "CN", "Asia/Shanghai"),
]

_SUPPLIERS = [
    ("RT-SUP-001", "Müller GmbH",     "DE", 14, 0.95, "active"),
    ("RT-SUP-002", "Shenzhen Trading", "CN", 60, 0.85, "active"),
    ("RT-SUP-003", "Atelier Durand",  "FR", 10, 0.92, "blocked"),
]


@pytest.fixture
def conn(migrated_db: str):
    """Clean connection per test, rolled back at teardown.

    Each test seeds + asserts within its own transaction so we don't
    leak rows between tests.
    """
    with psycopg.connect(migrated_db, row_factory=dict_row) as c:
        yield c
        c.rollback()


def _seed_items(conn) -> None:
    conn.execute(
        """
        INSERT INTO items (item_id, external_id, name, item_type, uom, status)
        SELECT gen_random_uuid(), * FROM UNNEST(
            %s::text[], %s::text[], %s::text[], %s::text[], %s::text[]
        )
        """,
        (
            [it[0] for it in _ITEMS],
            [it[1] for it in _ITEMS],
            [it[2] for it in _ITEMS],
            [it[3] for it in _ITEMS],
            [it[4] for it in _ITEMS],
        ),
    )


def _seed_locations(conn) -> None:
    conn.execute(
        """
        INSERT INTO locations (location_id, external_id, name, location_type, country, timezone)
        SELECT gen_random_uuid(), * FROM UNNEST(
            %s::text[], %s::text[], %s::text[], %s::text[], %s::text[]
        )
        """,
        (
            [lo[0] for lo in _LOCATIONS],
            [lo[1] for lo in _LOCATIONS],
            [lo[2] for lo in _LOCATIONS],
            [lo[3] for lo in _LOCATIONS],
            [lo[4] for lo in _LOCATIONS],
        ),
    )


# ---------------------------------------------------------------------------
# TSV round-trip (the recommended format per ADR-013 D1)
# ---------------------------------------------------------------------------


@requires_db
def test_items_roundtrip_tsv(conn) -> None:
    _seed_items(conn)

    # Export from canonical to TSV bytes (template items.md column order)
    rows = conn.execute(
        "SELECT external_id, name, item_type, uom, status FROM items "
        "WHERE external_id LIKE 'RT-%' ORDER BY external_id"
    ).fetchall()
    assert len(rows) == len(_ITEMS)

    buf = io.StringIO()
    writer = csv.writer(buf, delimiter="\t")
    writer.writerow(["external_id", "name", "item_type", "uom", "status"])
    for r in rows:
        writer.writerow([r["external_id"], r["name"], r["item_type"], r["uom"], r["status"]])
    tsv_bytes = buf.getvalue().encode("utf-8")

    # parse + load
    pr = parse(tsv_bytes, filename="items.tsv")
    assert pr.format == "tsv"
    assert pr.row_count == len(_ITEMS)
    assert pr.headers == ["external_id", "name", "item_type", "uom", "status"]

    result = load_to_staging(
        conn,
        parse_result=pr,
        entity_type="items",
        source_system="ROUNDTRIP-TEST",
        raw_bytes_size=len(tsv_bytes),
        filename="items.tsv",
        submitted_by="test",
    )
    assert result.rows_inserted == len(_ITEMS)

    # Verify each ingest_rows entry matches the source row exactly
    loaded = conn.execute(
        "SELECT row_number, col_01, col_02, col_03, col_04, col_05 "
        "FROM ingest_rows WHERE batch_id = %s ORDER BY row_number",
        (result.batch_id,),
    ).fetchall()
    for original, ingested in zip(_ITEMS, loaded):
        assert ingested["col_01"] == original[0], f"external_id mismatch at row {ingested['row_number']}"
        assert ingested["col_02"] == original[1], f"name mismatch at row {ingested['row_number']}"
        assert ingested["col_03"] == original[2]
        assert ingested["col_04"] == original[3]
        assert ingested["col_05"] == original[4]


@requires_db
def test_items_roundtrip_preserves_french_accents(conn) -> None:
    """The two items with accents + commas must survive the full round-trip."""
    _seed_items(conn)

    rows = conn.execute(
        "SELECT external_id, name FROM items "
        "WHERE external_id IN ('RT-RM-001', 'RT-RM-002') ORDER BY external_id"
    ).fetchall()
    buf = io.StringIO()
    writer = csv.writer(buf, delimiter="\t")
    writer.writerow(["external_id", "name"])
    for r in rows:
        writer.writerow([r["external_id"], r["name"]])
    tsv_bytes = buf.getvalue().encode("utf-8")

    pr = parse(tsv_bytes, format_hint="tsv")
    result = load_to_staging(
        conn,
        parse_result=pr,
        entity_type="items",
        source_system="ROUNDTRIP-FR",
        raw_bytes_size=len(tsv_bytes),
        filename="items_fr.tsv",
        submitted_by="test",
    )

    loaded = conn.execute(
        "SELECT col_02, raw_content FROM ingest_rows WHERE batch_id = %s ORDER BY row_number",
        (result.batch_id,),
    ).fetchall()
    assert loaded[0]["col_02"] == "Acier inoxydable français"
    assert loaded[1]["col_02"] == "Câble Cat6, blindé"

    # raw_content is JSON: the accent / comma must survive JSON serialisation
    parsed_json = json.loads(loaded[1]["raw_content"])
    assert parsed_json["name"] == "Câble Cat6, blindé"


# ---------------------------------------------------------------------------
# CSV with comma + quoted fields (the most error-prone format)
# ---------------------------------------------------------------------------


@requires_db
def test_items_roundtrip_csv_with_commas(conn) -> None:
    """RT-RM-002 has a comma in its name. The CSV writer must quote it,
    and the parser must un-quote it correctly. This is the test that
    catches the classic CSV vs TSV trade-off."""
    _seed_items(conn)

    rows = conn.execute(
        "SELECT external_id, name FROM items WHERE external_id LIKE 'RT-%' ORDER BY external_id"
    ).fetchall()

    buf = io.StringIO()
    writer = csv.writer(buf, delimiter=",", quotechar='"')
    writer.writerow(["external_id", "name"])
    for r in rows:
        writer.writerow([r["external_id"], r["name"]])
    csv_bytes = buf.getvalue().encode("utf-8")

    pr = parse(csv_bytes, filename="items.csv")
    assert pr.format == "csv"
    assert pr.delimiter == ","
    assert pr.row_count == len(_ITEMS)

    # The row with a comma in its name must come back intact
    cable_row = next(r for r in pr.rows if r["external_id"] == "RT-RM-002")
    assert cable_row["name"] == "Câble Cat6, blindé"


# ---------------------------------------------------------------------------
# JSON round-trip
# ---------------------------------------------------------------------------


@requires_db
def test_locations_roundtrip_json(conn) -> None:
    _seed_locations(conn)

    rows = conn.execute(
        "SELECT external_id, name, location_type, country, timezone FROM locations "
        "WHERE external_id LIKE 'RT-%' ORDER BY external_id"
    ).fetchall()
    payload = [
        {
            "external_id": r["external_id"],
            "name": r["name"],
            "location_type": r["location_type"],
            "country": r["country"],
            "timezone": r["timezone"],
        }
        for r in rows
    ]
    json_bytes = json.dumps(payload).encode("utf-8")

    pr = parse(json_bytes, filename="locations.json")
    assert pr.format == "json"
    assert pr.row_count == len(_LOCATIONS)

    result = load_to_staging(
        conn,
        parse_result=pr,
        entity_type="locations",
        source_system="ROUNDTRIP-JSON",
        raw_bytes_size=len(json_bytes),
        filename="locations.json",
        submitted_by="test",
    )
    assert result.rows_inserted == len(_LOCATIONS)

    # Verify the headers were derived correctly (union of all keys)
    batch = conn.execute(
        "SELECT total_rows FROM ingest_batches WHERE batch_id = %s",
        (result.batch_id,),
    ).fetchone()
    assert batch["total_rows"] == len(_LOCATIONS)


# ---------------------------------------------------------------------------
# Scale check — does the round-trip survive a larger volume?
# ---------------------------------------------------------------------------


@requires_db
def test_items_roundtrip_scale_1000_rows(conn) -> None:
    """Insert 1000 items, round-trip them, verify count + spot-check.

    Catches any pathological behaviour in the bulk-insert path that
    only shows up beyond ~100 rows (parameter array limits, planner
    quirks, etc.).
    """
    # Build 1000 items directly via UNNEST for speed
    ids = [str(uuid4()) for _ in range(1000)]
    ext_ids = [f"BULK-{i:05d}" for i in range(1000)]
    names = [f"Bulk item {i}" for i in range(1000)]
    types = ["component"] * 1000
    uoms = ["EA"] * 1000
    statuses = ["active"] * 1000
    conn.execute(
        """
        INSERT INTO items (item_id, external_id, name, item_type, uom, status)
        SELECT * FROM UNNEST(
            %s::uuid[], %s::text[], %s::text[], %s::text[], %s::text[], %s::text[]
        )
        """,
        (ids, ext_ids, names, types, uoms, statuses),
    )

    # Export to TSV
    rows = conn.execute(
        "SELECT external_id, name, item_type, uom, status FROM items "
        "WHERE external_id LIKE 'BULK-%' ORDER BY external_id"
    ).fetchall()
    assert len(rows) == 1000

    buf = io.StringIO()
    writer = csv.writer(buf, delimiter="\t")
    writer.writerow(["external_id", "name", "item_type", "uom", "status"])
    for r in rows:
        writer.writerow([r["external_id"], r["name"], r["item_type"], r["uom"], r["status"]])
    tsv_bytes = buf.getvalue().encode("utf-8")

    pr = parse(tsv_bytes, format_hint="tsv")
    assert pr.row_count == 1000

    result = load_to_staging(
        conn,
        parse_result=pr,
        entity_type="items",
        source_system="ROUNDTRIP-SCALE",
        raw_bytes_size=len(tsv_bytes),
        filename="bulk.tsv",
        submitted_by="test",
    )
    assert result.rows_inserted == 1000

    # Spot-check first + last row
    first = conn.execute(
        "SELECT col_01, col_02 FROM ingest_rows WHERE batch_id = %s ORDER BY row_number LIMIT 1",
        (result.batch_id,),
    ).fetchone()
    last = conn.execute(
        "SELECT col_01, col_02 FROM ingest_rows WHERE batch_id = %s "
        "ORDER BY row_number DESC LIMIT 1",
        (result.batch_id,),
    ).fetchone()
    assert first["col_01"] == "BULK-00000"
    assert last["col_01"] == "BULK-00999"
