"""
tests/test_outbound_export.py — Pure unit tests (no DB) for the outbound-export
TSV renderer (engine/reporting/outbound_export.py, ADR-042 decision 4, PR-5).
The DB half (load_pending_export_rows / execute_export / the CLI kill switch /
migration 085) lives in tests/integration/test_outbound_export_integration.py.

Four axes:

1. DETERMINISM, OCTET-EXACT — the renderer's hard contract: the SAME rows +
   run_date yield byte-identical TSV content, whether the same objects are
   rendered twice or the whole input tuple is rebuilt from scratch (fixed
   UUIDs); LF-only, exactly one trailing newline, no BOM character.

2. FAMILY ROUTING + COLUMNS — each action lands in its family file with the
   pinned LITERAL header (wording drift fails the build): po_drafts
   (ORDER_NOW/ORDER_RUSH/EXPEDITE; need_date = proposed_date else
   shortage_date), reschedule_messages (RESCHEDULE_IN/RESCHEDULE_OUT/CANCEL;
   target_po_reference = ERP external id, else raw node UUID, else empty
   cell), transfers (TRANSFER). File order, ``<family>_<AAAAMMJJ>.tsv``
   naming (zero-padded), per-file recommendation_ids in input order, and the
   fail-loud UnroutableExportActionError (DEFER/garbage names its offender —
   never a silently stranded row).

3. EMPTY FAMILY = NO FILE — a family with zero eligible rows produces no
   RenderedExportFile (never a header-only TSV); zero rows overall = zero
   files and an empty recommendation_ids tuple.

4. REGLES D'OR (pilot TSV rules, 2026-07-11/13) — tab-separated with NO
   quoting (quotes/commas pass through literally), ISO-8601 dates, an absent
   value is an EMPTY cell (never a literal 'None'/'NULL'), fixed-point
   quantities (no scientific notation, no thousands separator), and a
   business value carrying a structural tab/CR/LF fails loudly (ValueError)
   instead of silently corrupting the column alignment.

No DB required — this file must stay collectible and green without
DATABASE_URL.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from uuid import UUID

import pytest

from ootils_core.engine.reporting import (
    OutboundExportRender,
    PendingExportRow,
    RenderedExportFile,
    UnroutableExportActionError,
    render_outbound_export,
)

RUN_DATE = date(2026, 7, 18)

# Pinned literal headers — the outbound file contract (ADR-042 decision 1
# §Sortants). Deliberately NOT imported from the module: drift must fail here.
PO_HEADER = (
    "item_external_id\tsupplier_external_id\tquantity\tneed_date"
    "\taction\trecommendation_id\tconfidence"
)
RESCHEDULE_HEADER = (
    "item_external_id\ttarget_po_reference\tcurrent_receipt_date"
    "\tproposed_date\taction\trecommendation_id"
)
TRANSFERS_HEADER = (
    "item_external_id\tsource_location_external_id\tdest_location_external_id"
    "\tquantity\tshortage_date\trecommendation_id"
)


# ─────────────────────────────────────────────────────────────
# Deterministic input builders — fixed UUIDs so the SAME tuple can be rebuilt
# from scratch and must render byte-identically.
# ─────────────────────────────────────────────────────────────
def _uuid(n: int) -> UUID:
    return UUID(int=n)


def _row(n: int, action: str, **overrides) -> PendingExportRow:
    base = dict(
        recommendation_id=_uuid(n),
        action=action,
        item_external_id=f"IT-{n}",
        supplier_external_id=f"SUP-{n}",
        recommended_qty=Decimal("10"),
        shortage_date=date(2026, 8, 1),
        proposed_date=None,
        current_receipt_date=None,
        confidence="HIGH",
        target_node_id=None,
        target_po_external_id=None,
        source_location_external_id=None,
        dest_location_external_id=None,
    )
    base.update(overrides)
    return PendingExportRow(**base)


def _full_rows() -> tuple[PendingExportRow, ...]:
    """All three families, every column axis exercised: absent supplier,
    proposed-date coalesce, the 3-step PO-reference chain, decimal shapes."""
    return (
        # po_drafts family — input order EXPEDITE, ORDER_NOW, ORDER_RUSH
        # (matches the loader's ORDER BY action, recommendation_id).
        _row(1, "EXPEDITE", recommended_qty=Decimal("7.500")),
        _row(2, "ORDER_NOW", supplier_external_id=None),
        _row(3, "ORDER_RUSH", proposed_date=date(2026, 8, 9)),
        # reschedule_messages family — the 3-step reference chain.
        _row(
            4,
            "RESCHEDULE_IN",
            target_node_id=_uuid(0xA1),
            target_po_external_id="PO-ERP-77",
            current_receipt_date=date(2026, 9, 1),
            proposed_date=date(2026, 8, 15),
        ),
        _row(
            5,
            "RESCHEDULE_OUT",
            target_node_id=_uuid(0xA2),
            current_receipt_date=date(2026, 8, 5),
            proposed_date=date(2026, 9, 5),
        ),
        _row(6, "CANCEL"),
        # transfers family.
        _row(
            7,
            "TRANSFER",
            source_location_external_id="DC-EST",
            dest_location_external_id="DC-OUEST",
            recommended_qty=Decimal("120.500"),
            shortage_date=date(2026, 8, 20),
        ),
    )


def _file(render: OutboundExportRender, prefix: str) -> RenderedExportFile:
    matches = [f for f in render.files if f.filename.startswith(prefix)]
    assert len(matches) == 1, f"expected exactly one {prefix}* file"
    return matches[0]


def _body(f: RenderedExportFile) -> list[list[str]]:
    """Data rows as cell lists (header line excluded)."""
    return [line.split("\t") for line in f.content.splitlines()[1:]]


# ─────────────────────────────────────────────────────────────
# 1. Determinism, octet-exact
# ─────────────────────────────────────────────────────────────
class TestDeterminism:
    def test_byte_identical_same_objects_and_rebuilt_inputs(self):
        rows = _full_rows()
        r1 = render_outbound_export(rows, RUN_DATE)
        r2 = render_outbound_export(rows, RUN_DATE)  # same objects again
        r3 = render_outbound_export(_full_rows(), RUN_DATE)  # rebuilt graph
        for other in (r2, r3):
            assert [f.filename for f in other.files] == [f.filename for f in r1.files]
            assert [f.content.encode("utf-8") for f in other.files] == [
                f.content.encode("utf-8") for f in r1.files
            ]
        # Frozen-dataclass equality doubles as the full structural check
        # (filenames + byte-identical content + recommendation_ids).
        assert r1 == r2 == r3

    def test_lf_only_single_trailing_newline_no_bom(self):
        for f in render_outbound_export(_full_rows(), RUN_DATE).files:
            assert f.content.endswith("\n")
            assert not f.content.endswith("\n\n")
            assert "\r" not in f.content
            assert "\ufeff" not in f.content  # no BOM character, ever


# ─────────────────────────────────────────────────────────────
# 2. Family routing + columns
# ─────────────────────────────────────────────────────────────
class TestFamilyRoutingAndColumns:
    def test_file_order_and_dated_filenames(self):
        render = render_outbound_export(_full_rows(), RUN_DATE)
        assert [f.filename for f in render.files] == [
            "po_drafts_20260718.tsv",
            "reschedule_messages_20260718.tsv",
            "transfers_20260718.tsv",
        ]

    def test_filename_date_is_zero_padded(self):
        render = render_outbound_export((_row(1, "ORDER_NOW"),), date(2026, 1, 5))
        assert render.files[0].filename == "po_drafts_20260105.tsv"

    def test_po_drafts_columns_and_need_date_coalesce(self):
        f = _file(render_outbound_export(_full_rows(), RUN_DATE), "po_drafts_")
        assert f.content.splitlines()[0] == PO_HEADER
        body = _body(f)
        assert body == [
            # need_date = shortage_date when proposed_date is None …
            ["IT-1", "SUP-1", "7.500", "2026-08-01", "EXPEDITE", str(_uuid(1)), "HIGH"],
            ["IT-2", "", "10", "2026-08-01", "ORDER_NOW", str(_uuid(2)), "HIGH"],
            # … and proposed_date when present (forward-compat coalesce).
            ["IT-3", "SUP-3", "10", "2026-08-09", "ORDER_RUSH", str(_uuid(3)), "HIGH"],
        ]

    def test_reschedule_columns_and_po_reference_chain(self):
        f = _file(
            render_outbound_export(_full_rows(), RUN_DATE), "reschedule_messages_"
        )
        assert f.content.splitlines()[0] == RESCHEDULE_HEADER
        body = _body(f)
        assert body == [
            # ERP external id preferred …
            ["IT-4", "PO-ERP-77", "2026-09-01", "2026-08-15", "RESCHEDULE_IN", str(_uuid(4))],
            # … raw internal node UUID when the ERP never saw the order …
            ["IT-5", str(_uuid(0xA2)), "2026-08-05", "2026-09-05", "RESCHEDULE_OUT", str(_uuid(5))],
            # … honest empty cell when there is no target at all.
            ["IT-6", "", "", "", "CANCEL", str(_uuid(6))],
        ]

    def test_transfers_columns(self):
        f = _file(render_outbound_export(_full_rows(), RUN_DATE), "transfers_")
        assert f.content.splitlines()[0] == TRANSFERS_HEADER
        assert _body(f) == [
            ["IT-7", "DC-EST", "DC-OUEST", "120.500", "2026-08-20", str(_uuid(7))],
        ]

    def test_recommendation_ids_per_file_in_input_order_and_concatenated(self):
        render = render_outbound_export(_full_rows(), RUN_DATE)
        po, resched, transfers = render.files
        assert po.recommendation_ids == (_uuid(1), _uuid(2), _uuid(3))
        assert resched.recommendation_ids == (_uuid(4), _uuid(5), _uuid(6))
        assert transfers.recommendation_ids == (_uuid(7),)
        assert render.recommendation_ids == tuple(_uuid(n) for n in range(1, 8))

    def test_unroutable_action_raises_naming_the_offender(self):
        rows = (_row(1, "ORDER_NOW"), _row(9, "DEFER"))
        with pytest.raises(UnroutableExportActionError) as excinfo:
            render_outbound_export(rows, RUN_DATE)
        msg = str(excinfo.value)
        assert str(_uuid(9)) in msg
        assert "DEFER" in msg
        # The routable row must not mask the unroutable one.
        assert str(_uuid(1)) not in msg


# ─────────────────────────────────────────────────────────────
# 3. Empty family = no file
# ─────────────────────────────────────────────────────────────
class TestEmptyFamilyNoFile:
    def test_single_family_input_yields_single_file(self):
        render = render_outbound_export(
            (_row(1, "ORDER_NOW"), _row(2, "EXPEDITE")), RUN_DATE
        )
        assert [f.filename for f in render.files] == ["po_drafts_20260718.tsv"]

    def test_zero_rows_zero_files(self):
        render = render_outbound_export((), RUN_DATE)
        assert render.files == ()
        assert render.recommendation_ids == ()

    def test_never_a_header_only_file(self):
        for f in render_outbound_export(_full_rows(), RUN_DATE).files:
            assert len(f.content.splitlines()) >= 2  # header + >=1 data row


# ─────────────────────────────────────────────────────────────
# 4. Règles d'or (pilot TSV rules)
# ─────────────────────────────────────────────────────────────
class TestReglesDor:
    def test_tab_separated_no_quoting(self):
        # Quotes and commas are NOT structural in TSV — they pass through
        # literally, never wrapped in quoting the ERP side would misread.
        render = render_outbound_export(
            (_row(1, "ORDER_NOW", item_external_id='IT "spec", v2'),), RUN_DATE
        )
        cells = _body(render.files[0])[0]
        assert cells[0] == 'IT "spec", v2'
        assert len(cells) == 7

    def test_dates_are_iso_8601(self):
        render = render_outbound_export(_full_rows(), RUN_DATE)
        # Every rendered date cell is YYYY-MM-DD — pinned per file.
        assert _body(_file(render, "po_drafts_"))[0][3] == "2026-08-01"
        assert _body(_file(render, "reschedule_messages_"))[0][2:4] == [
            "2026-09-01", "2026-08-15",
        ]
        assert _body(_file(render, "transfers_"))[0][4] == "2026-08-20"
        # No non-ISO shape anywhere (dd/mm/yyyy or mm/dd/yyyy).
        for f in render.files:
            assert "/" not in f.content

    def test_absent_value_is_empty_cell_never_literal_none(self):
        render = render_outbound_export(_full_rows(), RUN_DATE)
        for f in render.files:
            assert "None" not in f.content
            assert "NULL" not in f.content
            header_len = len(f.content.splitlines()[0].split("\t"))
            for cells in _body(f):
                assert len(cells) == header_len  # empty cells keep alignment
        # The specific empty cells: IT-2's supplier, IT-6's reference + dates.
        po_row_2 = _body(_file(render, "po_drafts_"))[1]
        assert po_row_2[1] == ""
        cancel_row = _body(_file(render, "reschedule_messages_"))[2]
        assert cancel_row[1:4] == ["", "", ""]

    def test_quantities_fixed_point_no_scientific_notation(self):
        rows = (
            _row(1, "ORDER_NOW", recommended_qty=Decimal("1E+3")),
            _row(2, "ORDER_NOW", recommended_qty=Decimal("0.10")),
            _row(3, "ORDER_NOW", recommended_qty=Decimal("120.500")),
        )
        body = _body(render_outbound_export(rows, RUN_DATE).files[0])
        assert [cells[2] for cells in body] == ["1000", "0.10", "120.500"]
        content = render_outbound_export(rows, RUN_DATE).files[0].content
        assert "E+" not in content
        assert "," not in content  # no thousands separator either

    @pytest.mark.parametrize(
        "bad_value", ["SUP\t1", "SUP\n1", "SUP\r1"], ids=["tab", "lf", "cr"]
    )
    def test_structural_character_fails_loudly(self, bad_value):
        rows = (_row(1, "ORDER_NOW", supplier_external_id=bad_value),)
        with pytest.raises(ValueError, match="TSV-structural"):
            render_outbound_export(rows, RUN_DATE)
