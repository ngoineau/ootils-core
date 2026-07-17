"""
tests/test_ingest_status_lifecycle.py — Pure unit tests (no DB) for the
2026-07-16 ingest lifecycle fix.

Two axes:

1. Status → active mapping (terminal-status retraction). The whitelists
   `_PO_ACTIVE_STATUSES` / `_CO_ACTIVE_STATUSES` / `_TRANSFER_ACTIVE_STATUSES`
   in api/routers/ingest.py are the single source of "does this status still
   count in the projection". The expected impact tables below are transcribed
   from the CONTRACT docs (docs/contracts/TSV-FILES-SPEC.md §2.7 for POs,
   §2.8 for COs; docs/contracts/transfers/format-transfers-tsv.md for
   transfers — §2.10 of the spec carries no impact table) and pinned
   EXHAUSTIVELY against each entity's status enum: if either the enum or the
   whitelist drifts, a test here fails before the projection silently
   over/under-counts supply or demand again.

2. TSV quoting (QUOTE_NONE). TSV-FILES-SPEC.md §1.1 mandates "no quoting":
   a literal `"` in a cell (inch-mark item descriptions like
   `"U" BOLT 1/4-20 X 1-3/4`) must survive parsing verbatim. The three fixed
   parsers are exercised through their REAL public functions:
   scripts/ingest_file.py:parse_tsv, scripts/bulk_ingest.py:_read_tsv_header,
   and ootils_core.staging.parser.parse (TSV branch; the CSV branch keeps
   standard quoting — pinned too, it is a deliberate carve-out).

No DB required — this file must stay collectible and green without
DATABASE_URL.
"""
from __future__ import annotations

import os
import sys

import pytest
from pydantic import ValidationError

# auth.py validates OOTILS_API_TOKEN at IMPORT time — set it before importing
# anything that pulls in api.auth (same pattern as tests/test_auth_principal.py).
os.environ.setdefault("OOTILS_API_TOKEN", "test-token")

# scripts/ are not a package — same import pattern as tests/test_mrp_shim_compat.py.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import bulk_ingest  # noqa: E402
import ingest_file  # noqa: E402

from ootils_core.api.routers.ingest import (  # noqa: E402
    _CO_ACTIVE_STATUSES,
    _PO_ACTIVE_STATUSES,
    _TRANSFER_ACTIVE_STATUSES,
    VALID_CUSTOMER_ORDER_STATUSES,
    VALID_PO_STATUSES,
    VALID_TRANSFER_STATUSES,
    CustomerOrderRow,
    PurchaseOrderRow,
    TransferRow,
)
from ootils_core.staging.parser import parse as staging_parse  # noqa: E402


# ─────────────────────────────────────────────────────────────
# 1. Status → active impact tables (from the contract docs)
# ─────────────────────────────────────────────────────────────

# docs/contracts/TSV-FILES-SPEC.md §2.7 "Impact projection par statut"
PO_IMPACT: dict[str, bool] = {
    "draft": False,        # not yet committed supply
    "confirmed": True,     # expected receipt
    "in_transit": True,    # expected receipt
    "received": False,     # already folded into on_hand
    "cancelled": False,
}

# docs/contracts/TSV-FILES-SPEC.md §2.8 "Impact projection par statut"
CO_IMPACT: dict[str, bool] = {
    "open": True,          # soft demand
    "confirmed": True,     # firm demand
    "shipped": False,      # outflow already happened
    "delivered": False,
    "cancelled": False,
}

# docs/contracts/transfers/format-transfers-tsv.md (destination-PI receipt)
TRANSFER_IMPACT: dict[str, bool] = {
    "planned": True,       # expected receipt at destination
    "in_transit": True,    # expected receipt at destination
    "delivered": False,    # already folded into destination on_hand
    "cancelled": False,
}


class TestImpactTablesAreExhaustive:
    """The expected tables must cover each contract enum EXACTLY — a status
    added to (or removed from) the enum without a deliberate impact decision
    fails here."""

    def test_po_impact_table_covers_the_full_enum(self):
        assert set(PO_IMPACT) == VALID_PO_STATUSES

    def test_co_impact_table_covers_the_full_enum(self):
        assert set(CO_IMPACT) == VALID_CUSTOMER_ORDER_STATUSES

    def test_transfer_impact_table_covers_the_full_enum(self):
        assert set(TRANSFER_IMPACT) == VALID_TRANSFER_STATUSES


class TestStatusToActiveMapping:
    """The whitelist membership test IS the production mapping
    (`active = status in _X_ACTIVE_STATUSES` in each ingest endpoint) —
    assert it status by status against the contract impact tables."""

    @pytest.mark.parametrize("status", sorted(PO_IMPACT))
    def test_po_status_maps_to_contract_impact(self, status):
        assert (status in _PO_ACTIVE_STATUSES) == PO_IMPACT[status], (
            f"PO status '{status}': whitelist says active="
            f"{status in _PO_ACTIVE_STATUSES}, contract §2.7 says {PO_IMPACT[status]}"
        )

    @pytest.mark.parametrize("status", sorted(CO_IMPACT))
    def test_co_status_maps_to_contract_impact(self, status):
        assert (status in _CO_ACTIVE_STATUSES) == CO_IMPACT[status], (
            f"CO status '{status}': whitelist says active="
            f"{status in _CO_ACTIVE_STATUSES}, contract §2.8 says {CO_IMPACT[status]}"
        )

    @pytest.mark.parametrize("status", sorted(TRANSFER_IMPACT))
    def test_transfer_status_maps_to_contract_impact(self, status):
        assert (status in _TRANSFER_ACTIVE_STATUSES) == TRANSFER_IMPACT[status], (
            f"Transfer status '{status}': whitelist says active="
            f"{status in _TRANSFER_ACTIVE_STATUSES}, contract says "
            f"{TRANSFER_IMPACT[status]}"
        )

    def test_whitelists_are_subsets_of_their_enums(self):
        assert _PO_ACTIVE_STATUSES <= VALID_PO_STATUSES
        assert _CO_ACTIVE_STATUSES <= VALID_CUSTOMER_ORDER_STATUSES
        assert _TRANSFER_ACTIVE_STATUSES <= VALID_TRANSFER_STATUSES


class TestUnknownStatusIsInactiveByDefault:
    """A whitelist (vs the old `status != 'cancelled'` blacklist) makes the
    unknown case FAIL SAFE: anything outside the whitelist — typo, case
    variant, brand-new ERP status — counts as INACTIVE, never as phantom
    supply/demand in the projection."""

    WEIRD = ["", " ", "Confirmed", "CONFIRMED", "recieved", "in transit",
             "IN_TRANSIT", "open ", "Planned", "some_future_status"]

    @pytest.mark.parametrize("status", WEIRD)
    def test_unknown_status_is_inactive_for_all_three_entities(self, status):
        assert status not in _PO_ACTIVE_STATUSES
        assert status not in _CO_ACTIVE_STATUSES
        assert status not in _TRANSFER_ACTIVE_STATUSES


class TestModelBoundary:
    """Pin WHERE the invalid-status rejection happens per entity. CO,
    transfer AND purchase-order rows all carry a Pydantic validator (422 at
    the API edge) — a typo'd/out-of-enum status never reaches the whitelist
    silently. (Until 2026-07-17, PurchaseOrderRow deliberately had no
    validator and the whitelist's inactive-by-default was the only net;
    the DQ pipeline's L3_INVALID_PO_STATUS rule still runs independently
    as defense-in-depth for non-API ingestion paths, e.g. bulk COPY.)"""

    def test_customer_order_row_rejects_out_of_enum_status(self):
        with pytest.raises(ValidationError):
            CustomerOrderRow(
                external_id="CO-X", item_external_id="I", location_external_id="L",
                quantity=1, requested_delivery_date="2026-08-01", status="fulfilled",
            )

    def test_transfer_row_rejects_out_of_enum_status(self):
        with pytest.raises(ValidationError):
            TransferRow(
                external_id="TR-X", item_external_id="I",
                from_location_external_id="A", to_location_external_id="B",
                quantity=1, expected_delivery_date="2026-08-01", status="received",
            )

    def test_purchase_order_row_rejects_out_of_enum_status(self):
        with pytest.raises(ValidationError):
            PurchaseOrderRow(
                external_id="PO-X", item_external_id="I", location_external_id="L",
                supplier_external_id="S", quantity=1,
                expected_delivery_date="2026-08-01", status="not_a_status",
            )

    @pytest.mark.parametrize("status", sorted(VALID_PO_STATUSES))
    def test_purchase_order_row_accepts_every_enum_status(self, status):
        po = PurchaseOrderRow(
            external_id="PO-X", item_external_id="I", location_external_id="L",
            supplier_external_id="S", quantity=1,
            expected_delivery_date="2026-08-01", status=status,
        )
        assert po.status == status


# ─────────────────────────────────────────────────────────────
# 2. TSV quoting — literal `"` preserved verbatim (QUOTE_NONE)
# ─────────────────────────────────────────────────────────────

# The real pilot case: inch-mark item descriptions. With the csv module's
# default QUOTE_MINIMAL, the leading `"` used to open a quoted section and
# the parsers silently returned 'U BOLT 1/4-20 X 1-3/4'.
UBOLT = '"U" BOLT 1/4-20 X 1-3/4'
MIDQUOTE = 'PLAT 1/4" X 2"'


class TestIngestFileParseTsv:
    """scripts/ingest_file.py:parse_tsv — the API-path file loader."""

    def test_leading_and_mid_cell_quotes_preserved(self, tmp_path):
        p = tmp_path / "items.tsv"
        p.write_text(
            "external_id\tname\tuom\n"
            f"SKU-1\t{UBOLT}\tEA\n"
            f"SKU-2\t{MIDQUOTE}\tEA\n",
            encoding="utf-8",
        )
        headers, rows = ingest_file.parse_tsv(p)
        assert headers == ["external_id", "name", "uom"]
        assert rows[0]["name"] == UBOLT
        assert rows[1]["name"] == MIDQUOTE

    def test_quoted_cell_does_not_swallow_tab_separator(self, tmp_path):
        # Regression shape of the original bug: under QUOTE_MINIMAL a cell
        # opening with `"` kept consuming across the next `"` — here the two
        # columns must stay two columns, each verbatim.
        p = tmp_path / "items.tsv"
        p.write_text(
            'external_id\tname\n'
            'SKU-3\t"U" BOLT\n',
            encoding="utf-8",
        )
        headers, rows = ingest_file.parse_tsv(p)
        assert rows[0]["external_id"] == "SKU-3"
        assert rows[0]["name"] == '"U" BOLT'


class TestBulkIngestReadTsvHeader:
    """scripts/bulk_ingest.py:_read_tsv_header — the COPY-path header reader.
    (The data path already neutralized quoting via COPY ... QUOTE E'\\x01';
    the header reader was the remaining QUOTE_MINIMAL site.)"""

    def test_header_cell_with_literal_quotes_preserved(self, tmp_path):
        p = tmp_path / "weird.tsv"
        p.write_text(
            f'external_id\t{UBOLT}\tuom\n'
            "SKU-1\tx\tEA\n",
            encoding="utf-8",
        )
        headers = bulk_ingest._read_tsv_header(p)
        assert headers == ["external_id", UBOLT, "uom"]

    def test_plain_header_unchanged(self, tmp_path):
        p = tmp_path / "items.tsv"
        p.write_text("external_id\tname\tuom\nSKU-1\tPump\tEA\n", encoding="utf-8")
        assert bulk_ingest._read_tsv_header(p) == ["external_id", "name", "uom"]


class TestStagingParser:
    """ootils_core.staging.parser.parse — TSV branch loses quoting, CSV
    branch KEEPS standard `"`-quoting (deliberate carve-out: a comma-
    delimited value legitimately needs quotes)."""

    def test_tsv_preserves_literal_quotes(self):
        data = (
            "external_id\tname\tuom\n"
            f"SKU-1\t{UBOLT}\tEA\n"
        ).encode("utf-8")
        result = staging_parse(data, filename="items.tsv", format_hint="tsv")
        assert result.format == "tsv"
        assert result.rows[0]["name"] == UBOLT

    def test_tsv_mid_cell_quote_preserved(self):
        data = ("external_id\tname\n" f"SKU-2\t{MIDQUOTE}\n").encode("utf-8")
        result = staging_parse(data, format_hint="tsv")
        assert result.rows[0]["name"] == MIDQUOTE

    def test_csv_branch_still_honours_standard_quoting(self):
        # CSV carve-out: a quoted cell containing the delimiter must still
        # be parsed as ONE cell with the quotes consumed — untouched by the fix.
        data = b'external_id,name\nSKU-1,"BOLT, U-SHAPE"\n'
        result = staging_parse(data, filename="items.csv", format_hint="csv")
        assert result.format == "csv"
        assert result.rows[0]["name"] == "BOLT, U-SHAPE"
