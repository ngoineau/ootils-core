"""Unit tests for staging.parser — TSV/CSV/XLSX/JSON parsing.

Pure tests, no DB. Cover the format-detection, encoding-fallback, and
delimiter-sniffing paths plus the edge cases (empty file, no header,
duplicate headers, mixed sheets in XLSX).
"""
from __future__ import annotations

import io
import json

import pytest

from ootils_core.staging.parser import (
    ParseError,
    ParseOptions,
    parse,
)


# ---------------------------------------------------------------------------
# TSV
# ---------------------------------------------------------------------------


def test_tsv_basic() -> None:
    data = b"external_id\tname\titem_type\nSAP-001\tWidget A\tfinished_good\nSAP-002\tWidget B\tcomponent\n"
    r = parse(data, filename="items.tsv")
    assert r.format == "tsv"
    assert r.delimiter == "\t"
    assert r.encoding == "utf-8"
    assert r.headers == ["external_id", "name", "item_type"]
    assert r.row_count == 2
    assert r.rows[0] == {"external_id": "SAP-001", "name": "Widget A", "item_type": "finished_good"}


def test_tsv_with_blank_lines_ignored() -> None:
    data = b"a\tb\n\n1\t2\n\n3\t4\n"
    r = parse(data, format_hint="tsv")
    assert r.row_count == 2
    assert r.rows == [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]


def test_tsv_extension_tab() -> None:
    data = b"col1\tcol2\nv1\tv2\n"
    r = parse(data, filename="export.tab")
    assert r.format == "tsv"


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------


def test_csv_comma() -> None:
    data = b"external_id,name\nA1,Foo\nA2,Bar\n"
    r = parse(data, filename="items.csv")
    assert r.format == "csv"
    assert r.delimiter == ","
    assert r.rows[0]["name"] == "Foo"


def test_csv_semicolon_sniffed() -> None:
    data = b"external_id;name\nA1;Foo\nA2;Bar\n"
    r = parse(data, filename="items.csv")
    assert r.delimiter == ";"
    assert r.row_count == 2


def test_csv_with_quoted_comma_in_value() -> None:
    data = b'external_id,description\nA1,"hello, world"\nA2,plain\n'
    r = parse(data, filename="items.csv")
    assert r.rows[0]["description"] == "hello, world"


def test_csv_explicit_delimiter_override() -> None:
    data = b"a|b|c\n1|2|3\n"
    r = parse(data, format_hint="csv", options=ParseOptions(delimiter="|"))
    assert r.delimiter == "|"
    assert r.rows[0] == {"a": "1", "b": "2", "c": "3"}


# ---------------------------------------------------------------------------
# JSON
# ---------------------------------------------------------------------------


def test_json_array_of_objects() -> None:
    payload = [
        {"external_id": "A1", "name": "Foo", "qty": 10},
        {"external_id": "A2", "name": "Bar", "qty": 20},
    ]
    data = json.dumps(payload).encode("utf-8")
    r = parse(data, filename="items.json")
    assert r.format == "json"
    assert r.delimiter is None
    assert r.headers == ["external_id", "name", "qty"]
    # Scalar coercion to str
    assert r.rows[0]["qty"] == "10"


def test_json_invalid_raises() -> None:
    with pytest.raises(ParseError, match="invalid JSON"):
        parse(b"{not json", format_hint="json")


def test_json_top_level_object_rejected() -> None:
    with pytest.raises(ParseError, match="must be an array"):
        parse(b'{"a": 1}', format_hint="json")


def test_json_union_of_keys_as_headers() -> None:
    payload = [
        {"a": 1, "b": 2},
        {"a": 3, "c": 4},
    ]
    r = parse(json.dumps(payload).encode("utf-8"), format_hint="json")
    assert r.headers == ["a", "b", "c"]
    assert r.rows[0] == {"a": "1", "b": "2", "c": ""}
    assert r.rows[1] == {"a": "3", "b": "", "c": "4"}


# ---------------------------------------------------------------------------
# Encoding fallback
# ---------------------------------------------------------------------------


def test_cp1252_fallback() -> None:
    # 'café' encoded in CP-1252 isn't valid UTF-8 because of the 'é' byte 0xE9
    data = "external_id\tname\nA1\tcafé\n".encode("cp1252")
    r = parse(data, format_hint="tsv")
    assert r.encoding == "cp1252"
    assert r.rows[0]["name"] == "café"


def test_utf8_with_bom() -> None:
    data = "﻿external_id\tname\nA1\tFoo\n".encode("utf-8")
    r = parse(data, format_hint="tsv")
    assert r.encoding == "utf-8-sig"
    # The BOM must not contaminate the first header
    assert r.headers == ["external_id", "name"]


# ---------------------------------------------------------------------------
# Format detection edge cases
# ---------------------------------------------------------------------------


def test_format_detection_by_content_when_no_extension() -> None:
    # JSON sniffed by leading '['
    r = parse(b'[{"a": 1}]')
    assert r.format == "json"


def test_format_detection_tsv_by_tab_in_header() -> None:
    r = parse(b"col1\tcol2\nv1\tv2\n", filename="unknown")
    assert r.format == "tsv"


def test_format_detection_default_to_csv() -> None:
    r = parse(b"col1,col2\nv1,v2\n")
    assert r.format == "csv"


# ---------------------------------------------------------------------------
# Header validation
# ---------------------------------------------------------------------------


def test_empty_file_raises() -> None:
    with pytest.raises(ParseError, match="empty file"):
        parse(b"")


def test_no_header_raises() -> None:
    # Only blank lines
    with pytest.raises(ParseError, match="no header"):
        parse(b"\n\n\n", format_hint="tsv")


def test_empty_column_in_header_raises() -> None:
    with pytest.raises(ParseError, match="empty column"):
        parse(b"a\t\tc\n1\t2\t3\n", format_hint="tsv")


def test_duplicate_headers_raise() -> None:
    with pytest.raises(ParseError, match="duplicate"):
        parse(b"a\tA\nfoo\tbar\n", format_hint="tsv")


# ---------------------------------------------------------------------------
# Row shape robustness
# ---------------------------------------------------------------------------


def test_row_shorter_than_header_pads_with_empty() -> None:
    data = b"a\tb\tc\n1\t2\n3\t4\t5\n"
    r = parse(data, format_hint="tsv")
    assert r.rows[0] == {"a": "1", "b": "2", "c": ""}
    assert r.rows[1] == {"a": "3", "b": "4", "c": "5"}


def test_row_longer_than_header_truncates() -> None:
    data = b"a\tb\n1\t2\t3\t4\n"
    r = parse(data, format_hint="tsv")
    assert r.rows[0] == {"a": "1", "b": "2"}


def test_max_rows_limit() -> None:
    data = b"a\n" + b"\n".join(f"v{i}".encode() for i in range(10)) + b"\n"
    r = parse(data, format_hint="csv", options=ParseOptions(max_rows=3))
    assert r.row_count == 3


# ---------------------------------------------------------------------------
# SHA-256
# ---------------------------------------------------------------------------


def test_sha256_is_stable_for_identical_input() -> None:
    data = b"a\tb\n1\t2\n"
    a = parse(data, format_hint="tsv")
    b = parse(data, format_hint="tsv")
    assert a.sha256 == b.sha256


def test_sha256_differs_for_different_input() -> None:
    a = parse(b"a\tb\n1\t2\n", format_hint="tsv")
    b = parse(b"a\tb\n1\t3\n", format_hint="tsv")
    assert a.sha256 != b.sha256


# ---------------------------------------------------------------------------
# XLSX (requires openpyxl runtime)
# ---------------------------------------------------------------------------


@pytest.fixture
def xlsx_bytes() -> bytes:
    """Build a tiny in-memory xlsx with header + 2 rows."""
    openpyxl = pytest.importorskip("openpyxl")
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Sheet1"
    ws.append(["external_id", "name", "qty"])
    ws.append(["A1", "Foo", 10])
    ws.append(["A2", "Bar", 20])
    # Second sheet to test sheet selection
    ws2 = wb.create_sheet("Other")
    ws2.append(["col1"])
    ws2.append(["x"])
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def test_xlsx_default_first_sheet(xlsx_bytes: bytes) -> None:
    r = parse(xlsx_bytes, filename="items.xlsx")
    assert r.format == "xlsx"
    assert r.delimiter is None
    assert r.encoding is None
    assert r.headers == ["external_id", "name", "qty"]
    assert r.row_count == 2
    assert r.rows[0]["qty"] == "10"


def test_xlsx_sheet_name_override(xlsx_bytes: bytes) -> None:
    r = parse(xlsx_bytes, format_hint="xlsx", options=ParseOptions(sheet_name="Other"))
    assert r.headers == ["col1"]
    assert r.rows[0] == {"col1": "x"}


def test_xlsx_unknown_sheet_raises(xlsx_bytes: bytes) -> None:
    with pytest.raises(ParseError, match="not found"):
        parse(xlsx_bytes, format_hint="xlsx", options=ParseOptions(sheet_name="DoesNotExist"))


def test_xlsx_format_detected_by_pk_magic_without_extension(xlsx_bytes: bytes) -> None:
    r = parse(xlsx_bytes)
    assert r.format == "xlsx"
