"""
tests/test_ingest_distribution_links.py — unit tests (mocked DB, no Postgres)
for the DESC-1 PR-D delivery: POST /v1/ingest/distribution-links
(api/routers/ingest.py) + the TSV payload builder
(interfaces/ingest_exec.py:build_distribution_links_payload).

Pattern of tests/test_router_ingest.py: FakeDB/FakeCursor scripted cursors via
FastAPI dependency override — every branch of the router is driven without a
live database. The builder tests are pure (no app, no DB).

Coverage:
  Builder — full row conversion, minimal row (only the 3 required keys),
    generic lane (blank item omitted), '0' minimum_shipment_qty preserved (a
    legitimate 'no floor'), `active` NEVER emitted (out of the file contract,
    spec §8), required transit_lead_time_days (empty -> ValueError naming the
    line), numeric conversion errors naming field + line, dry_run passthrough.
  Router — auth 401; happy insert of 2 lanes (1 generic item_id NULL + 1
    item-specific) with server defaults (min 1, multiple 1, priority 100,
    active TRUE); upsert-update branch (existing triplet -> UPDATE, never a
    second INSERT); dry_run writes nothing; nominative 422s (unknown upstream /
    downstream / item, upstream == downstream, duplicate triplet in payload —
    including blank-item vs absent-item normalizing to the SAME generic key);
    Pydantic 422s (empty upstream, negative transit, transfer_multiple 0,
    priority 0); transit 0 valid (ge=0); blank item -> None via blank_to_none.
"""
from __future__ import annotations

import os
from uuid import uuid4

import pytest

# Auth token must be set BEFORE the app is imported (same guard as
# test_router_ingest.py — importing it below triggers create_app()).
os.environ.setdefault("OOTILS_API_TOKEN", "test-token")

from ootils_core.api.routers.ingest import (  # noqa: E402
    DistributionLinkRow,
    _distribution_link_key,
)
from ootils_core.interfaces.ingest_exec import (  # noqa: E402
    DISPATCH,
    PAYLOAD_BUILDERS,
    build_distribution_links_payload,
)

from tests.test_router_ingest import (  # noqa: E402
    AUTH_HEADERS,
    FakeCursor,
    FakeDB,
    make_client,
)


# ─────────────────────────────────────────────────────────────
# Builder: build_distribution_links_payload (pure, no DB)
# ─────────────────────────────────────────────────────────────


def _row(**overrides) -> dict[str, str]:
    base = {
        "__line__": "2",
        "upstream_external_id": "DC-A",
        "downstream_external_id": "DC-B",
        "item_external_id": "",
        "transit_lead_time_days": "7",
        "minimum_shipment_qty": "",
        "transfer_multiple": "",
        "priority": "",
    }
    base.update(overrides)
    return base


def test_builder_registered_in_dispatch_and_payload_builders():
    """The daily-orchestrator wiring: distribution_links.tsv routes to the new
    endpoint with body_key 'distribution_links' and to this builder."""
    assert DISPATCH["distribution_links.tsv"] == {
        "endpoint": "/v1/ingest/distribution-links",
        "body_key": "distribution_links",
    }
    assert PAYLOAD_BUILDERS["distribution_links.tsv"] is build_distribution_links_payload


def test_builder_full_row_converts_all_fields():
    payload = build_distribution_links_payload(
        [_row(item_external_id="SKU-1", minimum_shipment_qty="5",
              transfer_multiple="10", priority="2",
              transit_lead_time_days="7.5")],
        dry_run=False,
    )
    assert payload["dry_run"] is False
    assert payload["distribution_links"] == [{
        "upstream_external_id": "DC-A",
        "downstream_external_id": "DC-B",
        "item_external_id": "SKU-1",
        "transit_lead_time_days": 7.5,
        "minimum_shipment_qty": 5.0,
        "transfer_multiple": 10.0,
        "priority": 2,
    }]
    # priority is an int (server model expects int), the quantities floats.
    assert isinstance(payload["distribution_links"][0]["priority"], int)


def test_builder_minimal_row_emits_only_required_keys():
    """Blank optional cells are OMITTED (server defaults apply) and `active`
    is NEVER sent — it is out of the TSV file contract (spec §8)."""
    payload = build_distribution_links_payload([_row()], dry_run=False)
    assert set(payload["distribution_links"][0]) == {
        "upstream_external_id",
        "downstream_external_id",
        "transit_lead_time_days",
    }
    assert "active" not in payload["distribution_links"][0]


def test_builder_generic_lane_blank_item_omitted():
    """A blank/absent item cell = generic lane: the key is simply not sent
    (the server's blank_to_none would normalize it anyway; the builder never
    forwards an empty string)."""
    for row in (_row(item_external_id=""), {k: v for k, v in _row().items()
                                            if k != "item_external_id"}):
        payload = build_distribution_links_payload([row], dry_run=False)
        assert "item_external_id" not in payload["distribution_links"][0]


def test_builder_zero_minimum_shipment_qty_preserved():
    """'0' is a legitimate 'no floor' value — a truthy non-empty string, so it
    must flow through as 0.0, not be dropped as a blank."""
    payload = build_distribution_links_payload(
        [_row(minimum_shipment_qty="0")], dry_run=False
    )
    assert payload["distribution_links"][0]["minimum_shipment_qty"] == 0.0


def test_builder_missing_transit_raises_naming_line():
    """transit_lead_time_days is mandatory IN THIS FILE (§3 refuses inheriting
    the DB column's technical default silently)."""
    with pytest.raises(ValueError, match=r"line 9: transit_lead_time_days is required"):
        build_distribution_links_payload(
            [_row(transit_lead_time_days="", __line__="9")], dry_run=False
        )


def test_builder_non_numeric_transit_raises_naming_field_and_line():
    with pytest.raises(ValueError, match=r"line 4: transit_lead_time_days 'sept' is not a valid number"):
        build_distribution_links_payload(
            [_row(transit_lead_time_days="sept", __line__="4")], dry_run=False
        )


def test_builder_non_numeric_priority_raises_naming_field_and_line():
    with pytest.raises(ValueError, match=r"line 3: priority 'high' is not a valid integer"):
        build_distribution_links_payload(
            [_row(priority="high", __line__="3")], dry_run=False
        )


def test_builder_dry_run_passthrough():
    assert build_distribution_links_payload([], dry_run=True) == {
        "distribution_links": [],
        "dry_run": True,
    }


# ─────────────────────────────────────────────────────────────
# Pydantic model: blank item normalization + the payload dedup key
# ─────────────────────────────────────────────────────────────


def test_row_blank_item_normalized_to_none():
    """blank_to_none: a whitespace-only item_external_id IS a generic lane."""
    row = DistributionLinkRow(
        upstream_external_id="DC-A", downstream_external_id="DC-B",
        item_external_id="   ", transit_lead_time_days=7,
    )
    assert row.item_external_id is None
    # ... and it shares the SAME upsert key as an absent item (both generic).
    absent = DistributionLinkRow(
        upstream_external_id="DC-A", downstream_external_id="DC-B",
        transit_lead_time_days=7,
    )
    assert _distribution_link_key(row) == _distribution_link_key(absent) == (
        "DC-A", "DC-B", None
    )


# ─────────────────────────────────────────────────────────────
# Router: mock infrastructure
# ─────────────────────────────────────────────────────────────


def _links_db(loc_rows=None, item_rows=None, existing_link_id=None) -> FakeDB:
    """FakeDB router for /v1/ingest/distribution-links: serves the two
    _batch_existing map SELECTs and the SELECT-then-INSERT/UPDATE existence
    probe; every other statement gets a default cursor."""

    def handler(sql, params):
        if "FROM locations" in sql and "ANY" in sql:
            return FakeCursor(fetchall_value=list(loc_rows or []))
        if "FROM items" in sql and "ANY" in sql:
            return FakeCursor(fetchall_value=list(item_rows or []))
        if "SELECT distribution_link_id FROM distribution_links" in sql:
            if existing_link_id is not None:
                return FakeCursor(fetchone_value={"distribution_link_id": existing_link_id})
            return FakeCursor(fetchone_value=None)
        return FakeCursor()

    return FakeDB(handler=handler)


def _loc_rows(*external_ids):
    return [{"external_id": e, "location_id": uuid4()} for e in external_ids]


def _lane(**overrides) -> dict:
    base = {
        "upstream_external_id": "DC-A",
        "downstream_external_id": "DC-B",
        "transit_lead_time_days": 7,
    }
    base.update(overrides)
    return base


def _post(client, links, **body_extra):
    return client.post(
        "/v1/ingest/distribution-links",
        json={"distribution_links": links, **body_extra},
        headers=AUTH_HEADERS,
    )


# ─────────────────────────────────────────────────────────────
# Router: auth
# ─────────────────────────────────────────────────────────────


def test_ingest_distribution_links_requires_auth():
    client = make_client(FakeDB())
    resp = client.post(
        "/v1/ingest/distribution-links", json={"distribution_links": []}
    )
    assert resp.status_code == 401


# ─────────────────────────────────────────────────────────────
# Router: happy paths
# ─────────────────────────────────────────────────────────────


def test_happy_insert_generic_and_specific_lanes_coexist():
    """1 generic (item empty -> item_id NULL) + 1 item-specific lane on the
    SAME (upstream, downstream) pair: NOT duplicates of each other (spec §4),
    both inserted, server defaults applied where the row omitted a value."""
    locs = _loc_rows("DC-A", "DC-B")
    item_id = uuid4()
    db = _links_db(loc_rows=locs, item_rows=[{"external_id": "SKU-1", "item_id": item_id}])
    client = make_client(db)
    resp = _post(client, [
        _lane(item_external_id=""),               # generic lane, blank item
        _lane(item_external_id="SKU-1", minimum_shipment_qty=5,
              transfer_multiple=10, priority=2),  # item-specific lane
    ])
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "ok"
    assert body["summary"]["inserted"] == 2
    assert body["summary"]["updated"] == 0
    assert [r["action"] for r in body["results"]] == ["inserted", "inserted"]
    assert body["results"][0]["item_external_id"] is None      # generic
    assert body["results"][1]["item_external_id"] == "SKU-1"   # specific
    assert all(r["distribution_link_id"] for r in body["results"])

    inserts = [c for c in db.calls if "INSERT INTO distribution_links" in c[0]]
    assert len(inserts) == 2
    # INSERT params: (link_id, upstream, downstream, item_id, transit, min,
    # multiple, priority, active).
    generic, specific = inserts[0][1], inserts[1][1]
    assert generic[3] is None                      # item_id NULL = generic lane
    assert specific[3] == item_id
    # Server defaults where the row omitted the value (generic lane).
    assert generic[4:9] == (7.0, 1.0, 1.0, 100, True)
    # Explicit values flow through untouched (specific lane).
    assert specific[4:9] == (7.0, 5.0, 10.0, 2, True)


def test_happy_update_existing_lane_never_reinserts():
    existing_id = uuid4()
    db = _links_db(loc_rows=_loc_rows("DC-A", "DC-B"), existing_link_id=existing_id)
    client = make_client(db)
    resp = _post(client, [_lane(transit_lead_time_days=14)])
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["summary"]["updated"] == 1
    assert body["summary"]["inserted"] == 0
    assert body["results"][0]["action"] == "updated"
    assert body["results"][0]["distribution_link_id"] == str(existing_id)
    updates = [c for c in db.calls if "UPDATE distribution_links" in c[0]]
    assert len(updates) == 1
    # UPDATE params: (transit, min, multiple, priority, active, link_id).
    assert updates[0][1] == (14.0, 1.0, 1.0, 100, True, existing_id)
    assert not any("INSERT INTO distribution_links" in c[0] for c in db.calls)


def test_dry_run_validates_but_writes_nothing():
    db = _links_db(loc_rows=_loc_rows("DC-A", "DC-B"))
    client = make_client(db)
    resp = _post(client, [_lane()], dry_run=True)
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "dry_run"
    assert body["results"][0]["action"] == "dry_run"
    assert not any("INSERT INTO distribution_links" in c[0] for c in db.calls)
    assert not any("UPDATE distribution_links" in c[0] for c in db.calls)
    assert not any("SELECT distribution_link_id" in c[0] for c in db.calls)


def test_transit_zero_is_valid():
    """ge=0: a 0-day transit (same-campus lane) is legitimate."""
    db = _links_db(loc_rows=_loc_rows("DC-A", "DC-B"))
    client = make_client(db)
    resp = _post(client, [_lane(transit_lead_time_days=0)])
    assert resp.status_code == 200, resp.text
    assert resp.json()["summary"]["inserted"] == 1


# ─────────────────────────────────────────────────────────────
# Router: nominative 422s (server-side validation)
# ─────────────────────────────────────────────────────────────


def test_unknown_upstream_422_names_the_location():
    db = _links_db(loc_rows=_loc_rows("DC-B"))  # DC-A missing from DB
    client = make_client(db)
    resp = _post(client, [_lane()])
    assert resp.status_code == 422
    detail = resp.json()["detail"]
    assert len(detail) == 1
    assert detail[0]["row"] == 0
    assert any("upstream_external_id 'DC-A' not found in DB" in e
               for e in detail[0]["errors"])


def test_unknown_downstream_422_names_the_location():
    db = _links_db(loc_rows=_loc_rows("DC-A"))  # DC-B missing from DB
    client = make_client(db)
    resp = _post(client, [_lane()])
    assert resp.status_code == 422
    assert any("downstream_external_id 'DC-B' not found in DB" in e
               for e in resp.json()["detail"][0]["errors"])


def test_unknown_item_422_names_the_item():
    db = _links_db(loc_rows=_loc_rows("DC-A", "DC-B"), item_rows=[])
    client = make_client(db)
    resp = _post(client, [_lane(item_external_id="SKU-MISS")])
    assert resp.status_code == 422
    assert any("item_external_id 'SKU-MISS' not found in DB" in e
               for e in resp.json()["detail"][0]["errors"])


def test_same_upstream_and_downstream_422():
    db = _links_db(loc_rows=_loc_rows("DC-A"))
    client = make_client(db)
    resp = _post(client, [_lane(downstream_external_id="DC-A")])
    assert resp.status_code == 422
    assert any("must differ" in e for e in resp.json()["detail"][0]["errors"])


def test_duplicate_triplet_in_payload_422_points_at_first_row():
    """Two rows with the same (upstream, downstream, item) key — including a
    blank-item row vs an absent-item row, which BOTH normalize to the generic
    key — are refused, the error naming the first occurrence's row index."""
    db = _links_db(loc_rows=_loc_rows("DC-A", "DC-B"))
    client = make_client(db)
    resp = _post(client, [
        _lane(item_external_id=""),  # generic (blank item)
        _lane(),                     # generic (absent item) — SAME key
    ])
    assert resp.status_code == 422
    detail = resp.json()["detail"]
    assert detail[0]["row"] == 1
    assert any("duplicate" in e and "also at row 0" in e for e in detail[0]["errors"])


def test_generic_and_specific_rows_are_not_duplicates():
    """The dedup key includes the item: a generic row + a specific row on the
    same pair must NOT trip the duplicate check."""
    db = _links_db(
        loc_rows=_loc_rows("DC-A", "DC-B"),
        item_rows=[{"external_id": "SKU-1", "item_id": uuid4()}],
    )
    client = make_client(db)
    resp = _post(client, [_lane(), _lane(item_external_id="SKU-1")])
    assert resp.status_code == 200, resp.text


def test_all_or_nothing_one_bad_row_fails_the_batch():
    """Any error -> HTTP 422 and ZERO writes, even for the valid rows."""
    db = _links_db(loc_rows=_loc_rows("DC-A", "DC-B"))
    client = make_client(db)
    resp = _post(client, [
        _lane(),                                       # valid
        _lane(upstream_external_id="DC-GHOST"),        # unknown upstream
    ])
    assert resp.status_code == 422
    assert not any("INSERT INTO distribution_links" in c[0] for c in db.calls)


# ─────────────────────────────────────────────────────────────
# Router: Pydantic 422s (schema-level validation)
# ─────────────────────────────────────────────────────────────


def test_pydantic_empty_upstream_422():
    client = make_client(FakeDB())
    resp = _post(client, [_lane(upstream_external_id="  ")])
    assert resp.status_code == 422


def test_pydantic_negative_transit_422():
    client = make_client(FakeDB())
    resp = _post(client, [_lane(transit_lead_time_days=-1)])
    assert resp.status_code == 422


def test_pydantic_zero_transfer_multiple_422():
    """transfer_multiple is gt=0 — 0 would make the DOWN-rounding degenerate."""
    client = make_client(FakeDB())
    resp = _post(client, [_lane(transfer_multiple=0)])
    assert resp.status_code == 422


def test_pydantic_zero_priority_422():
    """priority is ge=1 (1 = most preferred)."""
    client = make_client(FakeDB())
    resp = _post(client, [_lane(priority=0)])
    assert resp.status_code == 422


def test_pydantic_zero_minimum_shipment_qty_is_valid():
    """ge=0: 0 is a legitimate 'no floor' minimum."""
    db = _links_db(loc_rows=_loc_rows("DC-A", "DC-B"))
    client = make_client(db)
    resp = _post(client, [_lane(minimum_shipment_qty=0)])
    assert resp.status_code == 200, resp.text
    inserts = [c for c in db.calls if "INSERT INTO distribution_links" in c[0]]
    assert inserts[0][1][5] == 0.0  # min_qty param — 0, not the default 1
