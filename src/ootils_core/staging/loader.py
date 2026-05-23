"""
loader.py — persist a parsed file into the staging tables (ADR-013 step 3).

Takes a `ParseResult` (from staging.parser) plus per-batch metadata and
writes three sets of rows in a single transaction:

  1. staging.uploads      one row per uploaded file (file metadata + sha)
  2. public.ingest_batches one row per logical batch (1-to-1 with upload
                          today, but the schema allows N batches per
                          upload if we ever need re-parsing variants)
  3. public.ingest_rows    one row per data row from the file, with raw
                          values in col_01..col_15 + the full row as a
                          JSON object in raw_content (so the DQ engine
                          can resolve column names that don't match
                          col_NN positionally)

The function is intentionally low-level: no HTTP concerns, no auth,
no DQ. It just persists. The upload endpoint (step 4) is the caller
that orchestrates parse -> load -> trigger-DQ.

Schema bounds:
  ingest_rows has col_01..col_15. We require headers <= 15 columns;
  files with more columns raise LoaderError with a clear message
  pointing at the schema limit. Future-proofing this would require a
  migration to add col_16..col_N.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from uuid import UUID, uuid4

import psycopg

from ootils_core.staging.parser import ParseResult


MAX_COLUMNS_PER_ROW = 15
"""Hard upper bound — matches ingest_rows.col_01..col_15 in migration 007."""


class LoaderError(ValueError):
    """Raised when the parse result cannot be loaded into staging."""


@dataclass(frozen=True)
class LoadResult:
    """What the loader actually wrote — returned to the caller for
    audit logging + response payload."""
    upload_id: UUID
    batch_id: UUID
    rows_inserted: int
    headers: list[str]
    format: str
    encoding: str | None
    delimiter: str | None
    sha256: str
    file_size_bytes: int


def load_to_staging(
    conn: psycopg.Connection,
    parse_result: ParseResult,
    entity_type: str,
    source_system: str,
    raw_bytes_size: int,
    filename: str,
    content_type: str | None = None,
    submitted_by: str | None = None,
    notes: str | None = None,
) -> LoadResult:
    """Persist a ParseResult into staging.

    Arguments:
        conn:             open psycopg connection. The function does NOT
                          commit — the caller decides the transaction
                          boundary.
        parse_result:     output of staging.parser.parse()
        entity_type:      must match ingest_batches.entity_type CHECK
                          constraint values (items / locations / suppliers / ...)
        source_system:    free-text identifier of the upstream system
                          (SAP-EU, KINAXIS, OPS-EXCEL-EXPORT, ...)
        raw_bytes_size:   size of the original file bytes (for staging.uploads)
        filename:         original filename (for staging.uploads)
        content_type:     declared Content-Type header (informational)
        submitted_by:     who initiated this upload (from auth context)
        notes:            free-text annotation stored on the batch

    Raises:
        LoaderError if the parse result violates staging contracts
        (too many columns, empty rows list, etc.)
    """
    if not parse_result.headers:
        raise LoaderError("parse_result has no headers (empty file)")
    if len(parse_result.headers) > MAX_COLUMNS_PER_ROW:
        raise LoaderError(
            f"file has {len(parse_result.headers)} columns, "
            f"but ingest_rows supports at most {MAX_COLUMNS_PER_ROW} "
            f"(col_01..col_{MAX_COLUMNS_PER_ROW:02d}). "
            "Split the file or extend the schema (migration)."
        )

    upload_id = uuid4()
    batch_id = uuid4()

    # ------------------------------------------------------------------
    # 1. staging.uploads
    # ------------------------------------------------------------------
    conn.execute(
        """
        INSERT INTO staging.uploads (
            upload_id, batch_id, filename, file_size_bytes, file_format,
            sha256, encoding, content_type, uploaded_by
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            upload_id, batch_id, filename, raw_bytes_size,
            parse_result.format, parse_result.sha256,
            parse_result.encoding, content_type, submitted_by,
        ),
    )

    # ------------------------------------------------------------------
    # 2. public.ingest_batches
    # ------------------------------------------------------------------
    conn.execute(
        """
        INSERT INTO ingest_batches (
            batch_id, entity_type, source_system, status,
            total_rows, submitted_by, notes
        ) VALUES (%s, %s, %s, 'pending', %s, %s, %s)
        """,
        (
            batch_id, entity_type, source_system,
            len(parse_result.rows), submitted_by, notes,
        ),
    )

    # ------------------------------------------------------------------
    # 3. public.ingest_rows — bulk INSERT via UNNEST
    # ------------------------------------------------------------------
    rows_inserted = 0
    if parse_result.rows:
        rows_inserted = _bulk_insert_rows(conn, batch_id, parse_result)

    return LoadResult(
        upload_id=upload_id,
        batch_id=batch_id,
        rows_inserted=rows_inserted,
        headers=list(parse_result.headers),
        format=parse_result.format,
        encoding=parse_result.encoding,
        delimiter=parse_result.delimiter,
        sha256=parse_result.sha256,
        file_size_bytes=raw_bytes_size,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _bulk_insert_rows(
    conn: psycopg.Connection,
    batch_id: UUID,
    parse_result: ParseResult,
) -> int:
    """Build 18 parallel arrays (row_id + row_number + raw_content + col_01..col_15)
    and INSERT via a single UNNEST statement."""
    n = len(parse_result.rows)
    headers = parse_result.headers
    width = len(headers)

    # Pre-allocate the column arrays
    row_ids: list[UUID] = [uuid4() for _ in range(n)]
    row_numbers: list[int] = list(range(1, n + 1))
    raw_contents: list[str] = []
    cols: list[list[str | None]] = [[None] * n for _ in range(MAX_COLUMNS_PER_ROW)]

    for row_idx, row in enumerate(parse_result.rows):
        # raw_content = the original row as a JSON dict (header -> value)
        raw_contents.append(json.dumps(row, ensure_ascii=False))
        # col_NN = positional value (1-indexed in DB, 0-indexed in array)
        for c_idx in range(width):
            cols[c_idx][row_idx] = row.get(headers[c_idx], "")

    # Build the UNNEST query — 18 arrays in, INSERT 18 columns out
    cur = conn.execute(
        """
        INSERT INTO ingest_rows (
            row_id, batch_id, row_number, raw_content,
            col_01, col_02, col_03, col_04, col_05,
            col_06, col_07, col_08, col_09, col_10,
            col_11, col_12, col_13, col_14, col_15
        )
        SELECT
            r.row_id, %s, r.row_number, r.raw_content,
            r.c01, r.c02, r.c03, r.c04, r.c05,
            r.c06, r.c07, r.c08, r.c09, r.c10,
            r.c11, r.c12, r.c13, r.c14, r.c15
        FROM UNNEST(
            %s::uuid[], %s::int[], %s::text[],
            %s::text[], %s::text[], %s::text[], %s::text[], %s::text[],
            %s::text[], %s::text[], %s::text[], %s::text[], %s::text[],
            %s::text[], %s::text[], %s::text[], %s::text[], %s::text[]
        ) AS r(
            row_id, row_number, raw_content,
            c01, c02, c03, c04, c05,
            c06, c07, c08, c09, c10,
            c11, c12, c13, c14, c15
        )
        """,
        (
            batch_id,
            row_ids, row_numbers, raw_contents,
            cols[0], cols[1], cols[2], cols[3], cols[4],
            cols[5], cols[6], cols[7], cols[8], cols[9],
            cols[10], cols[11], cols[12], cols[13], cols[14],
        ),
    )
    return cur.rowcount or 0
