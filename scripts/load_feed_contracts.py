"""
load_feed_contracts.py — feed-contract registry loader CLI (INT-1 PR1,
ADR-037).

Reads every ``config/feed-contracts/*.yaml`` file (interfaces/contracts.py:
``load_contract_dir`` — strict pydantic parse, cross-file validation: feed_key
uniqueness, ``depends_on`` referential integrity), then registers each as the
(possibly new) active version of its feed_key via ``upsert_contract``:
content-identical to the currently active version is a traced no-op; any
diff inserts a new version and supersedes the old one (migration 073). Run
it whenever ``config/feed-contracts/*.yaml`` changes — this is a REGISTRY
load, nothing here reads a contract at ingest time yet (that lands in
PR2/PR3).

Whole-directory transaction: ``load_contract_dir`` is pure/DB-free and runs
to completion BEFORE any DB write starts, so a single bad YAML file aborts
the whole run with nothing written — a partially-parsed directory never
partially registers. All the per-feed upserts inside the DB pass then share
ONE transaction (commit only after every feed_key has been processed).

Usage:
    DATABASE_URL=postgresql://... python scripts/load_feed_contracts.py \
        [--dir config/feed-contracts] [--dry-run] [--allow-dev]

Exit codes: 0 success (including a --dry-run that only validates); 1 config/
contract validation error; 2 missing DATABASE_URL or bad CLI args.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

import psycopg

import mrp_core as core

from ootils_core.interfaces.contracts import ContractError, load_contract_dir, upsert_contract

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("load_feed_contracts")

_DEFAULT_DIR = Path(__file__).resolve().parent.parent / "config" / "feed-contracts"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="Feed-contract registry loader (INT-1 PR1) — loads "
        "config/feed-contracts/*.yaml into the feed_contracts table."
    )
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument(
        "--dir", default=str(_DEFAULT_DIR),
        help="directory of *.yaml feed contracts (default: config/feed-contracts)",
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="parse + cross-validate only, no DB connection and no write",
    )
    p.add_argument("--allow-dev", action="store_true")
    args = p.parse_args(argv)

    contract_dir = Path(args.dir)
    try:
        specs = load_contract_dir(contract_dir)
    except ContractError as exc:
        logger.error("contract validation failed: %s", exc)
        return 1

    logger.info("parsed %d feed contract(s) from %s", len(specs), contract_dir)
    if not specs:
        logger.warning("no *.yaml files found under %s — nothing to load", contract_dir)

    if args.dry_run:
        for feed_key in sorted(specs):
            spec = specs[feed_key]
            logger.info(
                "  DRY-RUN %-24s entity_type=%-16s criticality=%-9s format=%s",
                feed_key, spec.entity_type, spec.criticality, spec.format,
            )
        return 0

    if not args.dsn:
        logger.error("DATABASE_URL not set")
        return 2

    db = core.guard_db(args.dsn, args.allow_dev)
    logger.info("Feed Contract Loader (INT-1 PR1) running on DB=%s", db)
    t0 = time.perf_counter()

    created = 0
    no_op = 0
    with psycopg.connect(args.dsn) as conn:
        for feed_key in sorted(specs):
            outcome = upsert_contract(conn, specs[feed_key])
            if outcome.action == "created":
                created += 1
                logger.info(
                    "  %-24s -> version %d (created, id=%s)",
                    feed_key, outcome.version, outcome.feed_contract_id,
                )
            else:
                no_op += 1
                logger.info(
                    "  %-24s -> version %d (unchanged, no-op)",
                    feed_key, outcome.version,
                )
        conn.commit()

    elapsed = round(time.perf_counter() - t0, 2)
    logger.info("=" * 92)
    logger.info("FEED CONTRACT LOAD — COMPLETED in %.2fs", elapsed)
    logger.info("  Contracts parsed  : %d", len(specs))
    logger.info("  New versions      : %d", created)
    logger.info("  Unchanged (no-op) : %d", no_op)
    logger.info("=" * 92)
    return 0


if __name__ == "__main__":
    sys.exit(main())
