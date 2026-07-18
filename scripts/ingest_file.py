"""
ingest_file.py — File-based ingestion entry point for Ootils.

Reads a TSV file from data/inbox/, parses it, calls the appropriate
/v1/ingest/<entity> endpoint via the FastAPI TestClient (in-process, no
HTTP server required), and archives the file to data/processed/ or
data/rejected/ with a JSON report alongside.

Usage:
    python scripts/ingest_file.py data/inbox/items.tsv
    python scripts/ingest_file.py data/inbox/items.tsv --dry-run

Accepted filenames (see `parse_ingest_filename` for the exact grammar):
    <entity>.tsv                     canonical (e.g. 'items.tsv')
    <entity>_<AAAAMMJJ>.tsv          daily drop, date ignored for dispatch
                                      (e.g. 'on_hand_20260718.tsv')
    <entity>.partNN.tsv              part file, siblings in the same
    <entity>.partNN_<AAAAMMJJ>.tsv   directory are grouped into ONE load
                                      (e.g. 'forecasts.part01.tsv')

Environment:
    DATABASE_URL       (required) PostgreSQL DSN
    OOTILS_API_TOKEN   (required) bearer token for in-process API auth

Currently supported entities (V1):
    items.tsv                 → POST /v1/ingest/items
    locations.tsv             → POST /v1/ingest/locations
    suppliers.tsv              → POST /v1/ingest/suppliers
    supplier_items.tsv        → POST /v1/ingest/supplier-items
    item_planning_params.tsv  → POST /v1/ingest/planning-params (SCD2 transparent)
    on_hand.tsv                → POST /v1/ingest/on-hand
    purchase_orders.tsv        → POST /v1/ingest/purchase-orders
    customer_orders.tsv        → POST /v1/ingest/customer-orders
    forecasts.tsv              → POST /v1/ingest/forecast-demand
    transfers.tsv               → POST /v1/ingest/transfers
    bom_header.tsv             → POST /v1/ingest/bom  (bundle mode — merges with bom_components.tsv, N calls)

See docs/contracts/<entity>/format-<entity>-tsv.md for each format spec.

ARCHITECTURE NOTE (ADR-042 PR-4b): the filename grammar, payload builders,
`call_api`, and `archive`/`archive_group` used below are NOT defined in this
file anymore — they live in the importable
`ootils_core.interfaces.ingest_exec` module (the canonical implementation,
shared with `engine/ingest/daily_orchestrator.py`'s governed daily run) and
are re-exported here so this script's CLI behaviour, and every object the
PR-4a test suite (`tests/test_ingest_file_naming.py`) imports/monkeypatches,
stay unchanged. This file keeps ONLY what is genuinely CLI-shaped:
`PROCESSED`/`REJECTED`/`INBOX` (repo-relative archive directories),
`_find_archived` (reads those same globals — kept local so the test suite's
`monkeypatch.setattr(ingest_file, "PROCESSED", ...)` still works),
`handle_bom_bundle`/`handle_part_group` (dry-run JSON printing + process
exit codes), and `main()`.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from ootils_core.interfaces.ingest_exec import (  # noqa: E402
    DISPATCH,
    PAYLOAD_BUILDERS,
    ParsedFilename,
    archive,
    archive_group,
    build_bom_payloads,
    call_api,
    find_sibling_parts,
    parse_ingest_filename,
    parse_tsv,
    parse_tsv_parts,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger("ingest_file")

INBOX = ROOT / "data" / "inbox"
PROCESSED = ROOT / "data" / "processed"
REJECTED = ROOT / "data" / "rejected"

__all__ = [
    "DISPATCH",
    "PAYLOAD_BUILDERS",
    "ParsedFilename",
    "archive",
    "archive_group",
    "build_bom_payloads",
    "call_api",
    "find_sibling_parts",
    "parse_ingest_filename",
    "parse_tsv",
    "parse_tsv_parts",
    "handle_bom_bundle",
    "handle_part_group",
    "main",
]


def _find_archived(missing_path: Path) -> Path | None:
    """If `missing_path` no longer exists at its original location, check
    whether a file matching `archive()`'s naming (`{stem}_{timestamp}{suffix}`)
    already exists in PROCESSED or REJECTED — i.e. this exact input was
    already consumed by a prior run. Returns the most recently modified
    match, or None if nothing matches (a genuine "not found").

    Only meaningful when `missing_path.exists()` is False; callers must
    check that first.
    """
    stem = missing_path.stem
    suffix = missing_path.suffix
    candidates: list[Path] = []
    for d in (PROCESSED, REJECTED):
        if d.exists():
            candidates.extend(d.glob(f"{stem}_*{suffix}"))
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


# ─────────────────────────────────────────────────────────────
# BOM bundle handler — 2 files (header + components) → N API calls (1 per BOM)
# ─────────────────────────────────────────────────────────────
def handle_bom_bundle(header_path: Path, dry_run: bool, token: str, *, date: str | None = None) -> int:
    """Bundle BOM ingestion: reads header + components from the same dir, emits N API calls.

    `date`, when set (dated-drop convention, e.g. 'bom_header_20260718.tsv'),
    picks the matching dated companion 'bom_components_<date>.tsv' instead
    of the canonical 'bom_components.tsv' — same date suffix on both sides
    of the bundle, ignored for dispatch otherwise. The '.partNN' convention
    is NOT supported for the BOM bundle (rejected by the caller in main()).

    Returns process exit code (0 if all BOMs OK, non-zero on any failure).
    """
    companion_name = f"bom_components_{date}.tsv" if date else "bom_components.tsv"
    components_path = header_path.parent / companion_name
    if not components_path.exists():
        logger.error(
            "%s not found next to %s. Both files must be present in the same directory: %s",
            companion_name, header_path.name, header_path.parent,
        )
        return 7

    started_at = datetime.now(timezone.utc).isoformat()
    endpoint = "/v1/ingest/bom"

    # ── 1. Parse both files ────────────────────────────────────
    try:
        _, header_rows = parse_tsv(header_path)
        _, component_rows = parse_tsv(components_path)
    except (FileNotFoundError, ValueError) as e:
        logger.error("parse error: %s", e)
        report = {
            "files": [header_path.name, companion_name],
            "started_at": started_at,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "outcome": "parse_error",
            "error": str(e),
        }
        if not dry_run:
            archive(header_path, REJECTED, report)
            if components_path.exists():
                archive(components_path, REJECTED, {"bundled_with": header_path.name, "see_report": "above"})
        else:
            print(json.dumps(report, indent=2, ensure_ascii=False))
        return 4

    # ── 2. Cross-file merge & validation ───────────────────────
    try:
        payloads = build_bom_payloads(header_rows, component_rows)
    except ValueError as e:
        logger.error("bundle validation error: %s", e)
        report = {
            "files": [header_path.name, companion_name],
            "started_at": started_at,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "outcome": "bundle_validation_error",
            "error": str(e),
            "header_rows": len(header_rows),
            "component_rows": len(component_rows),
        }
        if not dry_run:
            archive(header_path, REJECTED, report)
            if components_path.exists():
                archive(components_path, REJECTED, {"bundled_with": header_path.name})
        else:
            print(json.dumps(report, indent=2, ensure_ascii=False))
        return 4

    logger.info(
        "BOM bundle: %d header rows, %d component rows → %d BOMs to ingest",
        len(header_rows), len(component_rows), len(payloads),
    )

    # ── 3. Call API N times (1 per BOM) ────────────────────────
    bom_results: list[dict[str, Any]] = []
    all_ok = True
    for i, payload in enumerate(payloads, 1):
        body_with_dry = dict(payload)
        body_with_dry["dry_run"] = dry_run
        logger.info(
            "  [%d/%d] POST %s for parent=%s version=%s (%d components)",
            i, len(payloads), endpoint,
            payload["parent_external_id"], payload["bom_version"],
            len(payload["components"]),
        )
        try:
            status_code, body = call_api(endpoint, body_with_dry, token)
        except Exception as e:  # noqa: BLE001
            logger.exception("API call crashed for BOM #%d", i)
            bom_results.append({
                "parent_external_id": payload["parent_external_id"],
                "bom_version": payload["bom_version"],
                "outcome": "api_crash",
                "error": str(e),
            })
            all_ok = False
            continue
        accepted = 200 <= status_code < 300
        bom_results.append({
            "parent_external_id": payload["parent_external_id"],
            "bom_version": payload["bom_version"],
            "components_count": len(payload["components"]),
            "outcome": "ok" if accepted else "rejected",
            "http_status": status_code,
            "api_response": body,
        })
        if not accepted:
            all_ok = False

    # ── 4. Archive both files together ─────────────────────────
    outcome = "ok" if all_ok else "partial" if any(r["outcome"] == "ok" for r in bom_results) else "rejected"
    report = {
        "files": [header_path.name, companion_name],
        "endpoint": endpoint,
        "started_at": started_at,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "outcome": outcome,
        "dry_run": dry_run,
        "boms_total": len(payloads),
        "boms_ok": sum(1 for r in bom_results if r["outcome"] == "ok"),
        "boms_failed": sum(1 for r in bom_results if r["outcome"] != "ok"),
        "bom_results": bom_results,
    }

    if dry_run:
        logger.info("DRY-RUN — files not moved. Outcome: %s", outcome)
        print(json.dumps(report, indent=2, ensure_ascii=False))
        return 0 if all_ok else 1

    dest = PROCESSED if all_ok else REJECTED
    archive(header_path, dest, report)
    if components_path.exists():
        archive(components_path, dest, {"bundled_with": header_path.name, "see_main_report": True})
    logger.info(
        "%s — bundle archived to %s (%d BOMs, %d ok, %d failed)",
        outcome.upper(), dest, len(payloads),
        report["boms_ok"], report["boms_failed"],
    )
    if not all_ok:
        print(
            json.dumps(
                [r for r in bom_results if r["outcome"] != "ok"],
                indent=2, ensure_ascii=False,
            ),
            file=sys.stderr,
        )
    return 0 if all_ok else 1


# ─────────────────────────────────────────────────────────────
# Part-group handler — N sibling '.partNN' files -> ONE logical load
# ─────────────────────────────────────────────────────────────
def handle_part_group(first_part: Path, dry_run: bool, token: str) -> int:
    """Group `first_part` with all its same-(entity, date) siblings found in
    its own directory (see `find_sibling_parts`) and ingest them as ONE
    logical load — one concatenated payload, one POST, archived together.

    Returns process exit code (0 ok, non-zero on any failure), mirroring
    the single-file flow in main().
    """
    parsed = parse_ingest_filename(first_part.name)
    canonical = f"{parsed.entity}.tsv"
    cfg = DISPATCH[canonical]
    builder = PAYLOAD_BUILDERS[canonical]

    try:
        siblings = find_sibling_parts(first_part)
    except (FileNotFoundError, ValueError) as e:
        logger.error("part grouping error for '%s': %s", first_part.name, e)
        return 3

    started_at = datetime.now(timezone.utc).isoformat()
    logger.info(
        "ingesting %s (%d part(s): %s) → %s",
        canonical, len(siblings), [p.name for p in siblings], cfg["endpoint"],
    )

    # ── 1. Parse + concatenate parts ───────────────────────
    try:
        headers, rows = parse_tsv_parts(siblings)
    except (FileNotFoundError, ValueError) as e:
        logger.error("parse error: %s", e)
        report: dict[str, Any] = {
            "filename": first_part.name,
            "source_files": [p.name for p in siblings],
            "started_at": started_at,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "outcome": "parse_error",
            "error": str(e),
        }
        if not dry_run:
            archive_group(siblings, REJECTED, report)
        else:
            print(json.dumps(report, indent=2, ensure_ascii=False))
        return 4

    logger.info(
        "parsed %d rows from %d part(s) (header: %s)",
        len(rows), len(siblings), headers,
    )

    # ── 2. Build payload ───────────────────────────────────
    payload = builder(rows, dry_run)

    # ── 3. Call API ────────────────────────────────────────
    try:
        status_code, body = call_api(cfg["endpoint"], payload, token)
    except Exception as e:  # noqa: BLE001
        logger.exception("API call crashed")
        report = {
            "filename": first_part.name,
            "source_files": [p.name for p in siblings],
            "started_at": started_at,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "outcome": "api_crash",
            "error": str(e),
            "rows_parsed": len(rows),
        }
        if not dry_run:
            archive_group(siblings, REJECTED, report)
        else:
            print(json.dumps(report, indent=2, ensure_ascii=False))
        return 5

    # ── 4. Outcome + archive ──────────────────────────────
    accepted = 200 <= status_code < 300
    outcome = "ok" if accepted else "rejected"
    summary = body.get("summary", {}) if isinstance(body, dict) else {}

    report = {
        "filename": first_part.name,
        "source_files": [p.name for p in siblings],
        "endpoint": cfg["endpoint"],
        "started_at": started_at,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "outcome": outcome,
        "dry_run": dry_run,
        "http_status": status_code,
        "rows_parsed": len(rows),
        "parts_count": len(siblings),
        "api_summary": summary,
        "api_response": body,
    }

    if dry_run:
        logger.info("DRY-RUN — files not moved. Outcome: %s (HTTP %d)", outcome, status_code)
        print(json.dumps(report, indent=2, ensure_ascii=False))
        return 0 if accepted else 1

    dest = PROCESSED if accepted else REJECTED
    targets = archive_group(siblings, dest, report)
    logger.info(
        "%s — %d part(s) archived to %s",
        outcome.upper(), len(targets), dest,
    )
    if not accepted:
        print(json.dumps(body, indent=2, ensure_ascii=False), file=sys.stderr)
    return 0 if accepted else 1


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Ingest a TSV file from data/inbox/ into Ootils."
    )
    parser.add_argument("path", help="path to TSV file (typically under data/inbox/)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="validate but do not persist (no archive either)",
    )
    args = parser.parse_args(argv)

    src = Path(args.path).resolve()
    filename = src.name

    # Pre-flight: env vars
    token = os.environ.get("OOTILS_API_TOKEN")
    if not token:
        logger.error("OOTILS_API_TOKEN not set — refusing to run")
        return 2
    if not os.environ.get("DATABASE_URL"):
        logger.error("DATABASE_URL not set — refusing to run")
        return 2

    # Pre-flight: filename grammar (canonical / daily drop / part file — PR-4a)
    try:
        parsed = parse_ingest_filename(filename)
    except ValueError as e:
        logger.error("unsupported filename '%s': %s", filename, e)
        return 3

    if parsed.entity == "bom_components":
        logger.error(
            "'%s' cannot be ingested alone — it has no metadata. Use the "
            "matching 'bom_header[...].tsv' as the entry point; the script "
            "will auto-load the companion bom_components file from the "
            "same directory.",
            filename,
        )
        return 6

    canonical = f"{parsed.entity}.tsv"
    if canonical not in DISPATCH:
        logger.error(
            "unsupported filename '%s' (resolved entity '%s'). Supported entities: %s",
            filename, parsed.entity, sorted(k[: -len('.tsv')] for k in DISPATCH),
        )
        return 3

    # Pre-flight: was this exact input already consumed by a prior run? A
    # stale part re-run is the concrete case (PR-4a) but this generalizes to
    # any grammar — a clear "already archived" beats a generic parse_error.
    if not src.exists():
        prior = _find_archived(src)
        if prior is not None:
            logger.error(
                "'%s' no longer present at %s — already archived to '%s'. "
                "Nothing to do (re-running an already-consumed file/part is "
                "a no-op, not a retryable error).",
                filename, src.parent, prior,
            )
            return 8

    # ── BOM bundle: special path (2 files → N API calls) ──
    if parsed.entity == "bom_header":
        if parsed.part is not None:
            logger.error(
                "'%s' — the BOM bundle does not support the '.partNN' "
                "convention (bom_header/bom_components already pairs as a "
                "2-file bundle; use one pair per date instead).",
                filename,
            )
            return 3
        return handle_bom_bundle(src, args.dry_run, token, date=parsed.date)

    # ── Part file: group with siblings, ONE logical load ──
    if parsed.part is not None:
        return handle_part_group(src, args.dry_run, token)

    cfg = DISPATCH[canonical]
    builder = PAYLOAD_BUILDERS[canonical]

    started_at = datetime.now(timezone.utc).isoformat()
    logger.info("ingesting %s → %s", src, cfg["endpoint"])

    # ── 1. Parse ───────────────────────────────────────────
    try:
        headers, rows = parse_tsv(src)
    except (FileNotFoundError, ValueError) as e:
        logger.error("parse error: %s", e)
        report = {
            "filename": filename,
            "started_at": started_at,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "outcome": "parse_error",
            "error": str(e),
        }
        if not args.dry_run and src.exists():
            archive(src, REJECTED, report)
        else:
            print(json.dumps(report, indent=2, ensure_ascii=False))
        return 4

    logger.info("parsed %d rows (header: %s)", len(rows), headers)

    # ── 2. Build payload ───────────────────────────────────
    payload = builder(rows, args.dry_run)

    # ── 3. Call API ────────────────────────────────────────
    try:
        status_code, body = call_api(cfg["endpoint"], payload, token)
    except Exception as e:  # noqa: BLE001
        logger.exception("API call crashed")
        report = {
            "filename": filename,
            "started_at": started_at,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "outcome": "api_crash",
            "error": str(e),
            "rows_parsed": len(rows),
        }
        if not args.dry_run:
            archive(src, REJECTED, report)
        else:
            print(json.dumps(report, indent=2, ensure_ascii=False))
        return 5

    # ── 4. Outcome + archive ──────────────────────────────
    accepted = 200 <= status_code < 300
    outcome = "ok" if accepted else "rejected"
    summary = body.get("summary", {}) if isinstance(body, dict) else {}

    report = {
        "filename": filename,
        "endpoint": cfg["endpoint"],
        "started_at": started_at,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "outcome": outcome,
        "dry_run": args.dry_run,
        "http_status": status_code,
        "rows_parsed": len(rows),
        "api_summary": summary,
        "api_response": body,
    }

    if args.dry_run:
        logger.info("DRY-RUN — file not moved. Outcome: %s (HTTP %d)", outcome, status_code)
        print(json.dumps(report, indent=2, ensure_ascii=False))
        return 0 if accepted else 1

    dest = PROCESSED if accepted else REJECTED
    target, report_path = archive(src, dest, report)
    logger.info(
        "%s — archived to %s (report: %s)",
        outcome.upper(), target, report_path,
    )
    if not accepted:
        # Echo errors to stderr for ops visibility
        print(json.dumps(body, indent=2, ensure_ascii=False), file=sys.stderr)
    return 0 if accepted else 1


if __name__ == "__main__":
    sys.exit(main())
