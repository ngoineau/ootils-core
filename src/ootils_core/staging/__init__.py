"""
ootils_core.staging — external-data ingestion pipeline (ADR-013).

⚠️  DEPRECATED (ADR-042, 2026-07-18) — `api/routers/staging.py`, the only
HTTP surface for this package, is UNMOUNTED from the app (never wired past
`status='validated'` in production; superseded by the governed daily-run
pipeline). Package + `staging.*` tables are kept, not dropped — two guards
(`diff.DELETION_RATIO_THRESHOLD`, `reject.py`'s rejection-audit shape) were
relogged into the new pipeline, and `parser`/`diff` stay directly imported by
tests kept in the suite. See each submodule's own banner for detail. Do not
re-mount `staging.router`.

Submodules:
    parser    — unified TSV/CSV/XLSX/JSON parser with format + encoding detection
    loader    — parsed rows -> ingest_batches + ingest_rows (TBD)
    rules     — DQ L3/L4 business rules (TBD)
    approve   — full-reload transform + load on /approve (TBD)
"""
