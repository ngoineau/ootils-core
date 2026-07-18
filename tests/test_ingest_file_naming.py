"""
tests/test_ingest_file_naming.py — Pure unit tests (no DB) for the PR-4a
filename grammar and part-file grouping in scripts/ingest_file.py.

Three axes:

1. `parse_ingest_filename` — the pure basename parser. Canonical
   '<entity>.tsv', daily drop '<entity>_<AAAAMMJJ>.tsv', part file
   '<entity>.partNN.tsv', dated part '<entity>.partNN_<AAAAMMJJ>.tsv',
   plus the refusals (.tsv extension mandatory and case-sensitive, empty
   entity) and the deliberate tolerances (date is structural-only, a
   '.part' marker without >=2 digits is NOT a part marker — it stays in
   the entity and gets refused later at DISPATCH lookup, not by the
   parser).

2. `find_sibling_parts` — grouping of same-(entity, date) '.partNN'
   siblings in ONE directory: numeric part ordering (part02 < part003 <
   part10 — lexical order would differ), (entity, date) partitioning,
   noise immunity, duplicate-part-number refusal, and the hard error when
   the NAMED part is absent from its own directory (typo or already
   archived by a prior run).

3. `parse_tsv_parts` — concatenation of N parts into ONE logical load:
   single header (each part's own header line is consumed, never
   re-emitted as a data row), byte-identical header requirement (names
   AND order), `__line__` re-prefixed with the source file name for
   cross-part traceability.

No DB required — this file must stay collectible and green without
DATABASE_URL.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

# scripts/ are not a package — same import pattern as tests/test_ingest_status_lifecycle.py.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import ingest_file  # noqa: E402
from ingest_file import (  # noqa: E402
    DISPATCH,
    ParsedFilename,
    _find_archived,
    find_sibling_parts,
    parse_ingest_filename,
    parse_tsv_parts,
)


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────
_HEADER = "item_external_id\tqty"


def _write_tsv(path: Path, rows: list[str], header: str = _HEADER) -> Path:
    """Write a minimal TSV file (header + data rows, tab-separated)."""
    path.write_text("\n".join([header, *rows]) + "\n", encoding="utf-8")
    return path


# ─────────────────────────────────────────────────────────────
# 1. parse_ingest_filename — canonical names
# ─────────────────────────────────────────────────────────────
class TestParseCanonical:
    def test_items_tsv(self) -> None:
        assert parse_ingest_filename("items.tsv") == ParsedFilename(
            entity="items", date=None, part=None
        )

    @pytest.mark.parametrize("canonical", sorted(DISPATCH))
    def test_every_dispatch_key_round_trips(self, canonical: str) -> None:
        """Each supported canonical name parses to a bare entity whose
        '<entity>.tsv' re-derivation is exactly the DISPATCH key — the
        invariant main() relies on for dispatch."""
        parsed = parse_ingest_filename(canonical)
        assert f"{parsed.entity}.tsv" == canonical
        assert parsed.date is None
        assert parsed.part is None

    def test_entity_with_underscore_is_not_a_date(self) -> None:
        # 'supplier_items' has an underscore but no 8-digit suffix.
        parsed = parse_ingest_filename("supplier_items.tsv")
        assert parsed == ParsedFilename(entity="supplier_items", date=None, part=None)


# ─────────────────────────────────────────────────────────────
# 1b. parse_ingest_filename — daily-drop date suffix
# ─────────────────────────────────────────────────────────────
class TestParseDate:
    def test_dated_drop(self) -> None:
        parsed = parse_ingest_filename("on_hand_20260718.tsv")
        assert parsed == ParsedFilename(entity="on_hand", date="20260718", part=None)

    def test_entity_own_underscore_survives_date_stripping(self) -> None:
        parsed = parse_ingest_filename("supplier_items_20260718.tsv")
        assert parsed == ParsedFilename(
            entity="supplier_items", date="20260718", part=None
        )

    def test_short_digit_suffix_is_not_a_date(self) -> None:
        # 4 digits != AAAAMMJJ — the whole stem is the entity.
        parsed = parse_ingest_filename("items_2026.tsv")
        assert parsed == ParsedFilename(entity="items_2026", date=None, part=None)

    def test_date_is_structural_only_no_calendar_validation(self) -> None:
        # Same structural-only tolerance as `_to_date_str` (docstring pin):
        # 8 digits suffice, '00000000' is accepted as-is.
        parsed = parse_ingest_filename("items_00000000.tsv")
        assert parsed == ParsedFilename(entity="items", date="00000000", part=None)

    def test_only_last_8_digit_suffix_is_the_date(self) -> None:
        # Greedy stem: a prior 8-digit run stays in the entity.
        parsed = parse_ingest_filename("items_20260717_20260718.tsv")
        assert parsed == ParsedFilename(
            entity="items_20260717", date="20260718", part=None
        )


# ─────────────────────────────────────────────────────────────
# 1c. parse_ingest_filename — part marker
# ─────────────────────────────────────────────────────────────
class TestParsePart:
    def test_part_file(self) -> None:
        parsed = parse_ingest_filename("forecasts.part01.tsv")
        assert parsed == ParsedFilename(entity="forecasts", date=None, part=1)

    def test_two_digit_part(self) -> None:
        assert parse_ingest_filename("forecasts.part12.tsv").part == 12

    def test_more_than_two_digits_accepted(self) -> None:
        # NN means "at least two digits" — 'part007' is part 7.
        parsed = parse_ingest_filename("forecasts.part007.tsv")
        assert parsed == ParsedFilename(entity="forecasts", date=None, part=7)

    def test_dated_part(self) -> None:
        parsed = parse_ingest_filename("forecasts.part01_20260718.tsv")
        assert parsed == ParsedFilename(entity="forecasts", date="20260718", part=1)

    def test_single_digit_is_not_a_part_marker(self) -> None:
        # The grammar is '.partNN' (>=2 digits). 'part1' is NOT a part
        # marker: it stays inside the entity and is refused downstream at
        # DISPATCH lookup ('forecasts.part1.tsv' is not a known entity),
        # not by the parser.
        parsed = parse_ingest_filename("forecasts.part1.tsv")
        assert parsed == ParsedFilename(entity="forecasts.part1", date=None, part=None)
        assert f"{parsed.entity}.tsv" not in DISPATCH

    def test_digitless_part_is_not_a_part_marker(self) -> None:
        # Same logic: '.part' without digits is inert — the parser does
        # not raise; the unknown entity is refused at DISPATCH lookup.
        parsed = parse_ingest_filename("forecasts.part.tsv")
        assert parsed == ParsedFilename(entity="forecasts.part", date=None, part=None)
        assert f"{parsed.entity}.tsv" not in DISPATCH

    def test_reversed_date_part_ordering_not_recognized(self) -> None:
        # Only '<entity>.partNN_<date>.tsv' is a dated part. The reversed
        # '<entity>_<date>.partNN.tsv' leaves the date inside the entity
        # (and thus out of DISPATCH) — pin so the grammar can't silently
        # widen.
        parsed = parse_ingest_filename("forecasts_20260718.part01.tsv")
        assert parsed == ParsedFilename(entity="forecasts_20260718", date=None, part=1)


# ─────────────────────────────────────────────────────────────
# 1d. parse_ingest_filename — refusals & case sensitivity
# ─────────────────────────────────────────────────────────────
class TestParseRefusals:
    @pytest.mark.parametrize(
        "bad",
        ["items.csv", "items.tsv.gz", "items", "", "items.TSV", "items.Tsv"],
    )
    def test_non_tsv_extension_refused(self, bad: str) -> None:
        with pytest.raises(ValueError, match=r"\.tsv"):
            parse_ingest_filename(bad)

    def test_bare_extension_refused(self) -> None:
        with pytest.raises(ValueError, match="no entity segment"):
            parse_ingest_filename(".tsv")

    def test_entity_case_is_preserved_not_folded(self) -> None:
        # The parser is case-preserving; DISPATCH keys are lowercase, so
        # 'ITEMS.tsv' resolves to an unknown entity downstream.
        parsed = parse_ingest_filename("ITEMS.tsv")
        assert parsed.entity == "ITEMS"
        assert f"{parsed.entity}.tsv" not in DISPATCH


# ─────────────────────────────────────────────────────────────
# 2. find_sibling_parts — grouping
# ─────────────────────────────────────────────────────────────
class TestFindSiblingParts:
    def test_groups_all_parts_ascending_ignoring_noise(self, tmp_path: Path) -> None:
        p1 = _write_tsv(tmp_path / "forecasts.part01.tsv", ["A\t1"])
        p2 = _write_tsv(tmp_path / "forecasts.part02.tsv", ["B\t2"])
        p3 = _write_tsv(tmp_path / "forecasts.part03.tsv", ["C\t3"])
        # Noise: canonical file, other entity's part, dated part (other
        # group), non-TSV, and a DIRECTORY named like a part.
        _write_tsv(tmp_path / "forecasts.tsv", ["Z\t9"])
        _write_tsv(tmp_path / "items.part01.tsv", ["Y\t8"])
        _write_tsv(tmp_path / "forecasts.part01_20260718.tsv", ["X\t7"])
        (tmp_path / "notes.txt").write_text("nope", encoding="utf-8")
        (tmp_path / "forecasts.part09.tsv").mkdir()

        assert find_sibling_parts(p1) == [p1, p2, p3]

    def test_numeric_ordering_not_lexical(self, tmp_path: Path) -> None:
        # Lexically 'part003' < 'part02' < 'part10'; numerically 2 < 3 < 10.
        p10 = _write_tsv(tmp_path / "forecasts.part10.tsv", ["C\t3"])
        p3 = _write_tsv(tmp_path / "forecasts.part003.tsv", ["B\t2"])
        p2 = _write_tsv(tmp_path / "forecasts.part02.tsv", ["A\t1"])

        assert find_sibling_parts(p2) == [p2, p3, p10]

    def test_entry_via_any_member_returns_full_group(self, tmp_path: Path) -> None:
        p1 = _write_tsv(tmp_path / "forecasts.part01.tsv", ["A\t1"])
        p2 = _write_tsv(tmp_path / "forecasts.part02.tsv", ["B\t2"])

        assert find_sibling_parts(p2) == [p1, p2]

    def test_date_partitions_groups(self, tmp_path: Path) -> None:
        d1 = _write_tsv(tmp_path / "forecasts.part01_20260718.tsv", ["A\t1"])
        d2 = _write_tsv(tmp_path / "forecasts.part02_20260718.tsv", ["B\t2"])
        _write_tsv(tmp_path / "forecasts.part01.tsv", ["C\t3"])
        _write_tsv(tmp_path / "forecasts.part01_20260719.tsv", ["D\t4"])

        assert find_sibling_parts(d1) == [d1, d2]

    def test_gap_in_part_numbers_is_tolerated(self, tmp_path: Path) -> None:
        # The filename encodes no total part count, so a gap is NOT
        # detectable — grouping takes what is present. Pin the actual
        # semantics so a future gap-check is a deliberate change.
        p1 = _write_tsv(tmp_path / "forecasts.part01.tsv", ["A\t1"])
        p3 = _write_tsv(tmp_path / "forecasts.part03.tsv", ["C\t3"])

        assert find_sibling_parts(p1) == [p1, p3]

    def test_duplicate_part_number_refused(self, tmp_path: Path) -> None:
        # 'part01' and 'part001' both parse to part=1 — ambiguous.
        p1 = _write_tsv(tmp_path / "forecasts.part01.tsv", ["A\t1"])
        _write_tsv(tmp_path / "forecasts.part001.tsv", ["B\t2"])

        with pytest.raises(ValueError, match="duplicate part"):
            find_sibling_parts(p1)

    def test_named_part_absent_from_directory_is_hard_error(self, tmp_path: Path) -> None:
        # e.g. part01 was already archived by a prior run and only part02
        # remains: silently ingesting the leftovers would be wrong.
        _write_tsv(tmp_path / "forecasts.part02.tsv", ["B\t2"])
        missing = tmp_path / "forecasts.part01.tsv"  # never created

        with pytest.raises(ValueError, match="already archived"):
            find_sibling_parts(missing)

    def test_non_part_entry_point_refused(self, tmp_path: Path) -> None:
        p = _write_tsv(tmp_path / "forecasts.tsv", ["A\t1"])
        with pytest.raises(ValueError, match="not a '.partNN' file"):
            find_sibling_parts(p)

    def test_missing_directory_propagates(self, tmp_path: Path) -> None:
        ghost = tmp_path / "nope" / "forecasts.part01.tsv"
        with pytest.raises(FileNotFoundError):
            find_sibling_parts(ghost)


# ─────────────────────────────────────────────────────────────
# 3. parse_tsv_parts — concatenation into one logical load
# ─────────────────────────────────────────────────────────────
class TestParseTsvParts:
    def test_concatenates_with_single_header(self, tmp_path: Path) -> None:
        p1 = _write_tsv(tmp_path / "forecasts.part01.tsv", ["A\t1", "B\t2"])
        p2 = _write_tsv(tmp_path / "forecasts.part02.tsv", ["C\t3"])

        headers, rows = parse_tsv_parts([p1, p2])

        assert headers == ["item_external_id", "qty"]
        # Header dedup: part02's own header line is consumed, never
        # re-emitted as a data row.
        assert len(rows) == 3
        assert [r["item_external_id"] for r in rows] == ["A", "B", "C"]
        assert all(r["item_external_id"] != "item_external_id" for r in rows)

    def test_rows_keep_part_order(self, tmp_path: Path) -> None:
        # parse_tsv_parts trusts the caller's ordering (find_sibling_parts
        # hands it ascending) — rows come out part01 first, partNN last.
        p1 = _write_tsv(tmp_path / "forecasts.part01.tsv", ["A\t1"])
        p2 = _write_tsv(tmp_path / "forecasts.part02.tsv", ["B\t2"])
        p3 = _write_tsv(tmp_path / "forecasts.part03.tsv", ["C\t3"])

        _, rows = parse_tsv_parts([p1, p2, p3])
        assert [r["item_external_id"] for r in rows] == ["A", "B", "C"]

    def test_line_tags_are_prefixed_with_source_file(self, tmp_path: Path) -> None:
        p1 = _write_tsv(tmp_path / "forecasts.part01.tsv", ["A\t1", "B\t2"])
        p2 = _write_tsv(tmp_path / "forecasts.part02.tsv", ["C\t3"])

        _, rows = parse_tsv_parts([p1, p2])

        # Line numbers restart per file (header = line 1 in each part).
        assert rows[0]["__line__"] == "forecasts.part01.tsv:L2"
        assert rows[1]["__line__"] == "forecasts.part01.tsv:L3"
        assert rows[2]["__line__"] == "forecasts.part02.tsv:L2"

    def test_header_name_mismatch_refused_naming_offender(self, tmp_path: Path) -> None:
        p1 = _write_tsv(tmp_path / "forecasts.part01.tsv", ["A\t1"])
        p2 = _write_tsv(
            tmp_path / "forecasts.part02.tsv", ["B\t2"],
            header="item_external_id\tquantity",
        )

        with pytest.raises(ValueError, match="forecasts.part02.tsv"):
            parse_tsv_parts([p1, p2])

    def test_header_order_mismatch_refused(self, tmp_path: Path) -> None:
        # Byte-identical means same order too, not just same column set.
        p1 = _write_tsv(tmp_path / "forecasts.part01.tsv", ["A\t1"])
        p2 = _write_tsv(
            tmp_path / "forecasts.part02.tsv", ["2\tB"],
            header="qty\titem_external_id",
        )

        with pytest.raises(ValueError, match="does not match"):
            parse_tsv_parts([p1, p2])

    def test_empty_part_list_refused(self) -> None:
        with pytest.raises(ValueError, match="no part files"):
            parse_tsv_parts([])

    def test_empty_part_file_refused(self, tmp_path: Path) -> None:
        p1 = _write_tsv(tmp_path / "forecasts.part01.tsv", ["A\t1"])
        p2 = tmp_path / "forecasts.part02.tsv"
        p2.write_text("", encoding="utf-8")

        with pytest.raises(ValueError, match="empty"):
            parse_tsv_parts([p1, p2])

    def test_end_to_end_group_then_parse(self, tmp_path: Path) -> None:
        # The real pipeline: find_sibling_parts orders, parse_tsv_parts
        # concatenates — numeric part order decides row order.
        _write_tsv(tmp_path / "forecasts.part10.tsv", ["LAST\t3"])
        _write_tsv(tmp_path / "forecasts.part02.tsv", ["FIRST\t1"])
        p3 = _write_tsv(tmp_path / "forecasts.part003.tsv", ["MID\t2"])

        siblings = find_sibling_parts(p3)
        headers, rows = parse_tsv_parts(siblings)

        assert headers == ["item_external_id", "qty"]
        assert [r["item_external_id"] for r in rows] == ["FIRST", "MID", "LAST"]


# ─────────────────────────────────────────────────────────────
# 4. _find_archived — "already consumed" detection
# ─────────────────────────────────────────────────────────────
class TestFindArchived:
    @pytest.fixture()
    def archive_dirs(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        processed = tmp_path / "processed"
        rejected = tmp_path / "rejected"
        processed.mkdir()
        rejected.mkdir()
        monkeypatch.setattr(ingest_file, "PROCESSED", processed)
        monkeypatch.setattr(ingest_file, "REJECTED", rejected)
        return processed, rejected

    def test_finds_archived_copy_in_processed(self, tmp_path: Path, archive_dirs) -> None:
        processed, _ = archive_dirs
        # archive() naming: '{stem}_{timestamp}{suffix}'.
        archived = processed / "forecasts.part01_20260718_120000.tsv"
        archived.write_text("x", encoding="utf-8")

        missing = tmp_path / "inbox" / "forecasts.part01.tsv"
        assert _find_archived(missing) == archived

    def test_nothing_matching_returns_none(self, tmp_path: Path, archive_dirs) -> None:
        missing = tmp_path / "inbox" / "forecasts.part01.tsv"
        assert _find_archived(missing) is None

    def test_most_recent_match_wins(self, tmp_path: Path, archive_dirs) -> None:
        processed, rejected = archive_dirs
        older = processed / "items_20260717_080000.tsv"
        newer = rejected / "items_20260718_080000.tsv"
        older.write_text("old", encoding="utf-8")
        newer.write_text("new", encoding="utf-8")
        os.utime(older, (1_000_000_000, 1_000_000_000))
        os.utime(newer, (2_000_000_000, 2_000_000_000))

        missing = tmp_path / "inbox" / "items.tsv"
        assert _find_archived(missing) == newer
